use std::{
    cmp::min,
    fs,
    io::{Seek, SeekFrom, Write},
    sync::mpsc::{sync_channel, SyncSender},
    thread,
    time::Instant,
};

use pgrx::{
    direct_function_call,
    pg_sys::{
        self, BlockNumber, ForkNumber::MAIN_FORKNUM, HeapTuple, MemoryContext, Page, Pointer,
        BLCKSZ, RELSEG_SIZE,
    },
    IntoDatum, PgList, PgRelation, Spi,
};
use tpchgen::generators::{
    Customer, LineItem, LineItemGenerator, Nation, Order, Part, PartSupp, Region, Supplier,
};

const MAX_BLOCK_NUMBER: BlockNumber = 1024;

pub(crate) trait TpchTuple {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]);
}

pub(crate) struct LoadResult {
    pub rows: i64,
    pub heap_time_ms: f64,
    pub reindex_time_ms: f64,
}

pub(crate) fn load_rows<R, I>(table: &str, rows: I) -> LoadResult
where
    R: TpchTuple,
    I: IntoIterator<Item = R>,
{
    let mut loader = Loader::new(table);
    loader.generate_rows_and_load(rows)
}

pub(crate) fn load_lineitem_parallel(scale_factor: f64, workers: usize) -> LoadResult {
    let mut loader = ParallelLineItemLoader::new("lineitem");
    loader.generate_rows_and_load(scale_factor, workers)
}

const LINEITEM_NATTS: u16 = 16;
const LINEITEM_PAGE_BATCH_SIZE: usize = 128;

struct PageBatch {
    bytes: Vec<u8>,
    page_count: BlockNumber,
}

struct LineItemPageBuilder {
    xid: pg_sys::TransactionId,
    cid: pg_sys::CommandId,
    target_page_free_space: usize,
    current_page: Vec<u8>,
    current_page_has_rows: bool,
    tuple_buf: Vec<u8>,
    batch_bytes: Vec<u8>,
    batch_page_count: BlockNumber,
}

impl LineItemPageBuilder {
    fn new(
        xid: pg_sys::TransactionId,
        cid: pg_sys::CommandId,
        target_page_free_space: usize,
    ) -> Self {
        let mut current_page = vec![0_u8; BLCKSZ as usize];
        init_raw_page(&mut current_page);
        Self {
            xid,
            cid,
            target_page_free_space,
            current_page,
            current_page_has_rows: false,
            tuple_buf: Vec::with_capacity(512),
            batch_bytes: Vec::with_capacity(LINEITEM_PAGE_BATCH_SIZE * BLCKSZ as usize),
            batch_page_count: 0,
        }
    }

    fn push_row(&mut self, row: &LineItem<'_>, sender: &SyncSender<PageBatch>) {
        self.encode_tuple(row);
        let tuple_size = std::mem::size_of::<pg_sys::ItemIdData>()
            + align_up(self.tuple_buf.len(), std::mem::align_of::<u64>())
            + self.target_page_free_space;

        if self.current_page_has_rows && raw_page_free_space(&self.current_page) < tuple_size {
            self.finish_current_page(sender);
        }

        let offset = raw_page_add_item(&mut self.current_page, &self.tuple_buf);
        assert!(offset.is_some(), "lineitem tuple did not fit on empty page");
        self.current_page_has_rows = true;
    }

    fn finish(&mut self, sender: &SyncSender<PageBatch>) {
        if self.current_page_has_rows {
            self.finish_current_page(sender);
        }
        self.flush_batch(sender);
    }

    fn finish_current_page(&mut self, sender: &SyncSender<PageBatch>) {
        self.batch_bytes.extend_from_slice(&self.current_page);
        self.batch_page_count += 1;
        init_raw_page(&mut self.current_page);
        self.current_page_has_rows = false;

        if self.batch_page_count as usize >= LINEITEM_PAGE_BATCH_SIZE {
            self.flush_batch(sender);
        }
    }

    fn flush_batch(&mut self, sender: &SyncSender<PageBatch>) {
        if self.batch_page_count == 0 {
            return;
        }

        let bytes = std::mem::replace(
            &mut self.batch_bytes,
            Vec::with_capacity(LINEITEM_PAGE_BATCH_SIZE * BLCKSZ as usize),
        );
        let page_count = self.batch_page_count;
        self.batch_page_count = 0;
        sender.send(PageBatch { bytes, page_count }).unwrap();
    }

    fn encode_tuple(&mut self, row: &LineItem<'_>) {
        const HEAP_TUPLE_HEADER_LEN: usize =
            ((std::mem::offset_of!(pg_sys::HeapTupleHeaderData, t_bits) + 7) / 8) * 8;

        self.tuple_buf.clear();
        self.tuple_buf.resize(HEAP_TUPLE_HEADER_LEN, 0);

        append_i64_attr(&mut self.tuple_buf, row.l_orderkey);
        append_i64_attr(&mut self.tuple_buf, row.l_partkey);
        append_i64_attr(&mut self.tuple_buf, row.l_suppkey);
        append_i32_attr(&mut self.tuple_buf, row.l_linenumber);
        append_numeric_attr(&mut self.tuple_buf, row.l_quantity, 0);
        append_numeric_attr(&mut self.tuple_buf, row.l_extendedprice.into_inner(), 2);
        append_numeric_attr(&mut self.tuple_buf, row.l_discount.into_inner(), 2);
        append_numeric_attr(&mut self.tuple_buf, row.l_tax.into_inner(), 2);
        append_text_attr(&mut self.tuple_buf, row.l_returnflag);
        append_text_attr(&mut self.tuple_buf, row.l_linestatus);
        append_date_attr(&mut self.tuple_buf, row.l_shipdate);
        append_date_attr(&mut self.tuple_buf, row.l_commitdate);
        append_date_attr(&mut self.tuple_buf, row.l_receiptdate);
        append_text_attr(&mut self.tuple_buf, row.l_shipinstruct);
        append_text_attr(&mut self.tuple_buf, row.l_shipmode);
        append_text_attr(&mut self.tuple_buf, row.l_comment);

        unsafe {
            let tuple_header = self
                .tuple_buf
                .as_mut_ptr()
                .cast::<pg_sys::HeapTupleHeaderData>();
            (*tuple_header).t_infomask =
                (pg_sys::HEAP_HASVARWIDTH | pg_sys::HEAP_XMAX_INVALID) as u16;
            (*tuple_header).t_infomask2 = LINEITEM_NATTS;
            (*tuple_header).t_hoff = HEAP_TUPLE_HEADER_LEN as u8;
            crate::utils::PGUtils::heap_tuple_header_set_xmin(tuple_header, self.xid);
            crate::utils::PGUtils::heap_tuple_header_set_cmin(tuple_header, self.cid);
            crate::utils::PGUtils::heap_tuple_header_set_xmax(
                tuple_header,
                pg_sys::TransactionId::from(0),
            );
            (*tuple_header).t_ctid = pg_sys::ItemPointerData::default();
        }
    }
}

struct ParallelLineItemLoader {
    relation: PgRelation,
    rel_path: String,
    existing_blks: BlockNumber,
    total_blks: BlockNumber,
    checksums_enabled: bool,
    has_indexes: bool,
    wal_logged: bool,
    current_file: Option<std::fs::File>,
}

impl ParallelLineItemLoader {
    fn new(table: &str) -> Self {
        let relation = Loader::open_target_relation(table);
        Loader::validate_target_relation(&relation);
        Self {
            rel_path: relation_path(&relation),
            existing_blks: unsafe {
                pg_sys::RelationGetNumberOfBlocksInFork(relation.as_ptr(), MAIN_FORKNUM)
            },
            checksums_enabled: unsafe { pg_sys::DataChecksumsEnabled() },
            has_indexes: Loader::relation_has_indexes(&relation),
            relation,
            total_blks: 0,
            wal_logged: false,
            current_file: None,
        }
    }

    fn generate_rows_and_load(&mut self, scale_factor: f64, workers: usize) -> LoadResult {
        let worker_count = workers.max(1).min(i32::MAX as usize);
        let xid = unsafe { pg_sys::GetCurrentTransactionId() };
        let cid = unsafe { pg_sys::GetCurrentCommandId(true) };
        let target_page_free_space = crate::utils::PGUtils::relation_get_target_page_free_space(
            &self.relation,
            pg_sys::HEAP_DEFAULT_FILLFACTOR,
        );

        let heap_start = Instant::now();
        let (sender, receiver) = sync_channel::<PageBatch>(worker_count * 2);
        let mut handles = Vec::with_capacity(worker_count);
        for part in 0..worker_count {
            let sender = sender.clone();
            handles.push(thread::spawn(move || {
                std::panic::catch_unwind(|| {
                    run_lineitem_page_worker(
                        scale_factor,
                        (part + 1) as i32,
                        worker_count as i32,
                        xid,
                        cid,
                        target_page_free_space,
                        sender,
                    )
                })
            }));
        }
        drop(sender);

        for mut batch in receiver {
            self.write_batch(&mut batch.bytes, batch.page_count);
        }

        let mut inserted = 0_i64;
        for handle in handles {
            let result = handle.join().expect("lineitem page worker panicked");
            inserted += result.unwrap_or_else(|panic| {
                if let Some(message) = panic.downcast_ref::<&'static str>() {
                    panic!("{message}");
                }
                if let Some(message) = panic.downcast_ref::<String>() {
                    panic!("{message}");
                }
                panic!("lineitem page worker panicked");
            });
        }
        self.finish_heap_writes();
        let heap_time_ms = heap_start.elapsed().as_secs_f64() * 1000.0;

        let reindex_start = Instant::now();
        if self.has_indexes {
            let qualified_name = pgrx::spi::quote_qualified_identifier(
                self.relation.namespace(),
                self.relation.name(),
            );
            for (name, value) in Loader::aggressive_reindex_settings() {
                Spi::run(&format!("SET LOCAL {name} = '{value}'")).unwrap();
            }
            Spi::run(&format!("REINDEX TABLE {qualified_name}")).unwrap();
        }
        let reindex_time_ms = reindex_start.elapsed().as_secs_f64() * 1000.0;

        LoadResult {
            rows: inserted,
            heap_time_ms,
            reindex_time_ms,
        }
    }

    fn write_batch(&mut self, bytes: &mut [u8], page_count: BlockNumber) {
        if page_count == 0 {
            return;
        }

        for page_index in 0..page_count {
            let blk_num = self.current_blk_num() + page_index;
            let start = page_index as usize * BLCKSZ as usize;
            let end = start + BLCKSZ as usize;
            let page = bytes[start..end].as_mut_ptr().cast();
            unsafe { patch_page_ctids(page, blk_num) };
        }

        if !self.wal_logged {
            let page = bytes.as_mut_ptr().cast();
            unsafe {
                pg_sys::log_newpage(
                    &mut (*self.relation.as_ptr()).rd_locator as *mut pg_sys::RelFileLocator,
                    MAIN_FORKNUM,
                    self.current_blk_num(),
                    page,
                    true,
                );
            }
            self.wal_logged = true;
        }

        if self.checksums_enabled {
            for page_index in 0..page_count {
                let blk_num = self.current_blk_num() + page_index;
                let start = page_index as usize * BLCKSZ as usize;
                let end = start + BLCKSZ as usize;
                let page = bytes[start..end].as_mut_ptr().cast();
                unsafe {
                    pg_sys::PageSetChecksumInplace(page, blk_num);
                }
            }
        }

        let mut written_pages = 0;
        while written_pages < page_count {
            let blk_num = self.current_blk_num() + written_pages;
            if blk_num % RELSEG_SIZE == 0 {
                self.close_rel_file();
            }
            if self.current_file.is_none() {
                self.open_rel_file(blk_num);
            }

            let flush_num = min(
                page_count - written_pages,
                RELSEG_SIZE - blk_num % RELSEG_SIZE,
            );
            let start = written_pages as usize * BLCKSZ as usize;
            let end = start + flush_num as usize * BLCKSZ as usize;
            if let Some(file) = self.current_file.as_mut() {
                file.write_all(&bytes[start..end]).unwrap();
            }
            written_pages += flush_num;
        }

        self.total_blks += page_count;
    }

    fn finish_heap_writes(&mut self) {
        self.close_rel_file();
        let relation = self.relation.as_ptr();
        unsafe {
            if !(*relation).rd_smgr.is_null() {
                (*(*relation).rd_smgr).smgr_cached_nblocks[MAIN_FORKNUM as usize] =
                    self.current_blk_num();
            }
            pg_sys::CacheInvalidateRelcache(relation);
        }
    }

    fn close_rel_file(&mut self) {
        if let Some(mut f) = self.current_file.take() {
            f.flush().unwrap();
        }
    }

    fn open_rel_file(&mut self, blk_num: BlockNumber) {
        let mut path = self.rel_path.clone();
        let segno = blk_num / RELSEG_SIZE;
        if segno > 0 {
            path = format!("{path}.{segno}");
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start((BLCKSZ * (blk_num % RELSEG_SIZE)) as u64))
            .unwrap();
        self.current_file = Some(file);
    }

    fn current_blk_num(&self) -> BlockNumber {
        self.existing_blks + self.total_blks
    }
}

fn run_lineitem_page_worker(
    scale_factor: f64,
    part: i32,
    part_count: i32,
    xid: pg_sys::TransactionId,
    cid: pg_sys::CommandId,
    target_page_free_space: usize,
    sender: SyncSender<PageBatch>,
) -> i64 {
    let generator = LineItemGenerator::new(scale_factor, part, part_count);
    let mut builder = LineItemPageBuilder::new(xid, cid, target_page_free_space);
    let mut rows = 0_i64;
    for row in generator.iter() {
        builder.push_row(&row, &sender);
        rows += 1;
    }
    builder.finish(&sender);
    rows
}

fn relation_path(relation: &PgRelation) -> String {
    unsafe {
        let locator = (*relation).rd_locator;
        let path = pg_sys::GetRelationPath(
            locator.dbOid,
            locator.spcOid,
            locator.relNumber,
            pg_sys::INVALID_PROC_NUMBER as i32,
            MAIN_FORKNUM,
        );
        core::ffi::CStr::from_ptr(path.str_.as_ptr())
            .to_string_lossy()
            .into_owned()
    }
}

unsafe fn patch_page_ctids(page: Page, blk_num: BlockNumber) {
    let max_offset = pg_sys::PageGetMaxOffsetNumber(page);
    for offset in 1..=max_offset {
        let item_id = pg_sys::PageGetItemId(page, offset);
        let item = pg_sys::PageGetItem(page, item_id) as pg_sys::HeapTupleHeader;
        let mut tid = pg_sys::ItemPointerData::default();
        pg_sys::ItemPointerSet(&mut tid as *mut pg_sys::ItemPointerData, blk_num, offset);
        (*item).t_ctid = tid;
    }
}

fn align_up(value: usize, align: usize) -> usize {
    (value + (align - 1)) & !(align - 1)
}

fn init_raw_page(page: &mut [u8]) {
    page.fill(0);
    let header = unsafe { &mut *(page.as_mut_ptr().cast::<pg_sys::PageHeaderData>()) };
    header.pd_lower = std::mem::offset_of!(pg_sys::PageHeaderData, pd_linp) as u16;
    header.pd_upper = BLCKSZ as u16;
    header.pd_special = BLCKSZ as u16;
    header.pd_pagesize_version = BLCKSZ as u16 | pg_sys::PG_PAGE_LAYOUT_VERSION as u16;
}

fn raw_page_free_space(page: &[u8]) -> usize {
    let header = unsafe { &*(page.as_ptr().cast::<pg_sys::PageHeaderData>()) };
    header.pd_upper as usize - header.pd_lower as usize
}

fn raw_page_add_item(page: &mut [u8], item: &[u8]) -> Option<pg_sys::OffsetNumber> {
    let header = unsafe { &mut *(page.as_mut_ptr().cast::<pg_sys::PageHeaderData>()) };
    let item_id_size = std::mem::size_of::<pg_sys::ItemIdData>();
    let aligned_len = align_up(item.len(), std::mem::align_of::<u64>());
    let lower = header.pd_lower as usize;
    let upper = header.pd_upper as usize;
    let new_upper = upper.checked_sub(aligned_len)?;
    let new_lower = lower.checked_add(item_id_size)?;

    if new_lower > new_upper {
        return None;
    }

    page[new_upper..new_upper + item.len()].copy_from_slice(item);
    if aligned_len > item.len() {
        page[new_upper + item.len()..new_upper + aligned_len].fill(0);
    }

    let base = std::mem::offset_of!(pg_sys::PageHeaderData, pd_linp);
    let offset = ((lower - base) / item_id_size + 1) as pg_sys::OffsetNumber;
    let item_id_ptr = unsafe {
        page.as_mut_ptr()
            .add(base)
            .cast::<pg_sys::ItemIdData>()
            .add(offset as usize - 1)
    };
    unsafe {
        (*item_id_ptr).set_lp_off(new_upper as u32);
        (*item_id_ptr).set_lp_flags(pg_sys::LP_NORMAL);
        (*item_id_ptr).set_lp_len(item.len() as u32);
    }

    header.pd_lower = new_lower as u16;
    header.pd_upper = new_upper as u16;
    Some(offset)
}

fn append_aligned_bytes(dst: &mut Vec<u8>, align: usize, bytes: &[u8]) {
    dst.resize(align_up(dst.len(), align), 0);
    dst.extend_from_slice(bytes);
}

fn append_i64_attr(dst: &mut Vec<u8>, value: i64) {
    append_aligned_bytes(dst, std::mem::align_of::<i64>(), &value.to_ne_bytes());
}

fn append_i32_attr(dst: &mut Vec<u8>, value: i32) {
    append_aligned_bytes(dst, std::mem::align_of::<i32>(), &value.to_ne_bytes());
}

fn append_date_attr(dst: &mut Vec<u8>, value: tpchgen::dates::TPCHDate) {
    append_i32_attr(dst, crate::encoding::tpch_date_to_pg_date(value));
}

fn append_text_attr(dst: &mut Vec<u8>, value: &str) {
    dst.resize(align_up(dst.len(), std::mem::align_of::<i32>()), 0);
    crate::encoding::append_text_bytes(dst, value);
}

fn append_numeric_attr(dst: &mut Vec<u8>, value: i64, scale: u16) {
    dst.resize(align_up(dst.len(), std::mem::align_of::<i32>()), 0);
    crate::encoding::append_numeric_i64_with_scale_bytes(dst, value, scale);
}

pub(crate) struct Loader {
    buffer_pool: Pointer,
    max_buffered_blocks: BlockNumber,
    current_page: BlockNumber,
    total_blks: BlockNumber,
    existing_blks: BlockNumber,
    relation: PgRelation,
    tuple_desc: pg_sys::TupleDesc,
    rel_path: String,
    target_page_free_space: usize,
    checksums_enabled: bool,
    has_indexes: bool,
    current_file: Option<std::fs::File>,
    memctx: MemoryContext,
    xid: pg_sys::TransactionId,
    cid: pg_sys::CommandId,
    datums: Vec<pg_sys::Datum>,
    nulls: Vec<bool>,
    wal_logged: bool,
}

impl Loader {
    pub fn new(table: &str) -> Self {
        let relation = Self::open_target_relation(table);
        Self::validate_target_relation(&relation);
        let has_indexes = Self::relation_has_indexes(&relation);

        let natts = unsafe { (*relation.tuple_desc().as_ptr()).natts as usize };
        let max_buffered_blocks = Self::recommended_buffered_blocks();
        let tuple_desc = relation.tuple_desc().as_ptr();
        let target_page_free_space = crate::utils::PGUtils::relation_get_target_page_free_space(
            &relation,
            pg_sys::HEAP_DEFAULT_FILLFACTOR,
        );
        let rel_path = unsafe {
            let locator = (*relation).rd_locator;
            let path = pg_sys::GetRelationPath(
                locator.dbOid,
                locator.spcOid,
                locator.relNumber,
                pg_sys::INVALID_PROC_NUMBER as i32,
                MAIN_FORKNUM,
            );
            core::ffi::CStr::from_ptr(path.str_.as_ptr())
                .to_string_lossy()
                .into_owned()
        };
        let checksums_enabled = unsafe { pg_sys::DataChecksumsEnabled() };

        let memctx = unsafe {
            pg_sys::AllocSetContextCreateInternal(
                pg_sys::CurrentMemoryContext,
                b"tpch_tuple_memctx\0".as_ptr() as *const std::ffi::c_char,
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        };

        let loader = Loader {
            buffer_pool: unsafe {
                pg_sys::palloc((BLCKSZ * max_buffered_blocks) as usize) as Pointer
            },
            max_buffered_blocks,
            current_page: 0,
            total_blks: 0,
            existing_blks: unsafe {
                pg_sys::RelationGetNumberOfBlocksInFork(relation.as_ptr(), MAIN_FORKNUM)
            },
            relation,
            tuple_desc,
            rel_path,
            target_page_free_space,
            checksums_enabled,
            has_indexes,
            current_file: None,
            memctx,
            xid: unsafe { pg_sys::GetCurrentTransactionId() },
            cid: unsafe { pg_sys::GetCurrentCommandId(true) },
            datums: vec![pg_sys::Datum::from(0); natts],
            nulls: vec![false; natts],
            wal_logged: false,
        };

        unsafe { Self::init_page(loader.get_current_page()) };

        loader
    }

    fn open_target_relation(table: &str) -> PgRelation {
        unsafe {
            match direct_function_call::<pg_sys::Oid>(pg_sys::to_regclass, &[table.into_datum()]) {
                Some(oid) => {
                    PgRelation::with_lock(oid, pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE)
                }
                None => panic!("no such relation"),
            }
        }
    }

    fn validate_target_relation(relation: &PgRelation) {
        let relkind = unsafe { (*(*relation).rd_rel).relkind as u8 };
        if relkind != pg_sys::RELKIND_RELATION {
            panic!("raw heap loading only supports ordinary heap tables");
        }
    }

    fn relation_has_indexes(relation: &PgRelation) -> bool {
        let index_list = unsafe {
            PgList::<pg_sys::Oid>::from_pg(pg_sys::RelationGetIndexList(relation.as_ptr()))
        };
        !index_list.is_empty()
    }

    pub fn generate_rows_and_load<R, I>(&mut self, rows: I) -> LoadResult
    where
        R: TpchTuple,
        I: IntoIterator<Item = R>,
    {
        let heap_start = Instant::now();
        let mut inserted = 0_i64;
        unsafe {
            let old_ctx = pg_sys::MemoryContextSwitchTo(self.memctx);
            for row in rows {
                self.load_row(row);
                inserted += 1;
            }
            pg_sys::MemoryContextSwitchTo(old_ctx);
            self.flush();
        }
        let heap_time_ms = heap_start.elapsed().as_secs_f64() * 1000.0;

        let reindex_start = Instant::now();
        if self.has_indexes {
            self.reindex_target_relation();
        }
        let reindex_time_ms = reindex_start.elapsed().as_secs_f64() * 1000.0;

        LoadResult {
            rows: inserted,
            heap_time_ms,
            reindex_time_ms,
        }
    }

    fn reindex_target_relation(&self) {
        let qualified_name =
            pgrx::spi::quote_qualified_identifier(self.relation.namespace(), self.relation.name());
        for (name, value) in Self::aggressive_reindex_settings() {
            Spi::run(&format!("SET LOCAL {name} = '{value}'")).unwrap();
        }
        Spi::run(&format!("REINDEX TABLE {qualified_name}")).unwrap();
    }

    fn aggressive_reindex_settings() -> Vec<(&'static str, String)> {
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4);
        let parallel_maintenance_workers = parallelism.saturating_sub(1).clamp(1, 8);
        let max_parallel_workers = parallelism.clamp(1, 16);
        let maintenance_work_mem_mb = Self::recommended_maintenance_work_mem_mb();
        let io_concurrency = if cfg!(target_family = "unix") { 256 } else { 0 };

        vec![
            (
                "maintenance_work_mem",
                format!("{maintenance_work_mem_mb}MB"),
            ),
            (
                "max_parallel_maintenance_workers",
                parallel_maintenance_workers.to_string(),
            ),
            ("max_parallel_workers", max_parallel_workers.to_string()),
            ("maintenance_io_concurrency", io_concurrency.to_string()),
            ("effective_io_concurrency", io_concurrency.to_string()),
            ("parallel_leader_participation", "on".to_string()),
            ("synchronous_commit", "off".to_string()),
        ]
    }

    fn recommended_maintenance_work_mem_mb() -> usize {
        let total_mb = Self::detect_total_memory_mb().unwrap_or(4096);
        (total_mb / 3).clamp(1024, 8192)
    }

    fn detect_total_memory_mb() -> Option<usize> {
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
        let kb = line.split_whitespace().nth(1)?.parse::<usize>().ok()?;
        Some(kb / 1024)
    }

    fn recommended_buffered_blocks() -> BlockNumber {
        let total_mb = Self::detect_total_memory_mb().unwrap_or(4096);
        let target_buffer_mb = (total_mb / 64).clamp(8, 64);
        let blocks = target_buffer_mb.saturating_mul(1024 / 8);
        blocks.clamp(MAX_BLOCK_NUMBER as usize, 8192) as BlockNumber
    }

    fn close_rel_file(&mut self) {
        if let Some(mut f) = self.current_file.take() {
            f.flush().unwrap();
            std::mem::drop(f);
        }
    }

    fn open_rel_file(&mut self, blk_num: BlockNumber) {
        let mut path = self.rel_path.clone();

        let segno = blk_num / RELSEG_SIZE;
        if segno > 0 {
            path = format!("{path}.{segno}");
        }

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        file.seek(SeekFrom::Start((BLCKSZ * (blk_num % RELSEG_SIZE)) as u64))
            .unwrap();

        self.current_file = Some(file);
    }

    unsafe fn load_row<R: TpchTuple>(&mut self, row: R) {
        row.write_datums(&mut self.datums);
        let tuple = pg_sys::heap_form_tuple(
            self.tuple_desc,
            self.datums.as_mut_ptr(),
            self.nulls.as_mut_ptr(),
        );
        let switched = self.load_tuple(tuple);
        pg_sys::heap_freetuple(tuple);

        if switched {
            pg_sys::MemoryContextReset(self.memctx);
        }
    }

    unsafe fn load_tuple(&mut self, tuple: HeapTuple) -> bool {
        let mut page = self.get_current_page();
        let tuple_size = std::mem::size_of::<pg_sys::ItemIdData>()
            + pg_sys::MAXALIGN((*tuple).t_len as usize)
            + self.target_page_free_space;

        let mut switched = false;
        if pg_sys::PageGetFreeSpace(page) < tuple_size {
            switched = true;
            if self.current_page < self.max_buffered_blocks - 1 {
                self.current_page += 1;
            } else {
                self.flush();
                self.current_page = 0;
            }

            Self::init_page(self.get_current_page());
            page = self.get_current_page();
        }

        let values = (*tuple).t_data;
        (*values).t_infomask &= !(pg_sys::HEAP_XACT_MASK as u16);
        (*values).t_infomask2 &= !(pg_sys::HEAP2_XACT_MASK as u16);
        (*values).t_infomask |= pg_sys::HEAP_XMAX_INVALID as u16;

        crate::utils::PGUtils::heap_tuple_header_set_xmin(values, self.xid);
        crate::utils::PGUtils::heap_tuple_header_set_cmin(values, self.cid);
        crate::utils::PGUtils::heap_tuple_header_set_xmax(values, pg_sys::TransactionId::from(0));

        let offset = pg_sys::PageAddItemExtended(
            page,
            values as pg_sys::Item,
            (*tuple).t_len as usize,
            pg_sys::InvalidOffsetNumber,
            pg_sys::PAI_IS_HEAP as i32,
        );
        assert_ne!(offset, pg_sys::InvalidOffsetNumber);

        let mut tid = (*tuple).t_self;
        pg_sys::ItemPointerSet(
            &mut tid as *mut pg_sys::ItemPointerData,
            self.current_blk_num() + self.current_page,
            offset,
        );
        let item_id = pg_sys::PageGetItemId(page, offset);
        let item = pg_sys::PageGetItem(page, item_id) as pg_sys::HeapTupleHeader;
        (*item).t_ctid = tid;
        switched
    }

    unsafe fn flush(&mut self) {
        let mut num_pages = self.current_page;
        if !pg_sys::PageIsEmpty(self.get_current_page()) {
            num_pages += 1;
        }

        if num_pages == 0 {
            return;
        }

        let relation = self.relation.as_ptr();

        if !self.wal_logged {
            let blk_num = self.current_blk_num();
            let page = self.get_target_page(0);
            pg_sys::log_newpage(
                &mut (*relation).rd_locator as *mut pg_sys::RelFileLocator,
                MAIN_FORKNUM,
                blk_num,
                page,
                true,
            );
            self.wal_logged = true;
        }

        let mut written_pages = 0;
        while written_pages < num_pages {
            let blk_num = self.current_blk_num() + written_pages;
            if blk_num % RELSEG_SIZE == 0 {
                self.close_rel_file();
            }
            if self.current_file.is_none() {
                self.open_rel_file(blk_num);
            }

            let flush_num = min(
                num_pages - written_pages,
                RELSEG_SIZE - blk_num % RELSEG_SIZE,
            );
            if self.checksums_enabled {
                for page_offset in 0..flush_num {
                    let page = self.get_target_page(written_pages + page_offset);
                    pg_sys::PageSetChecksumInplace(page, blk_num + page_offset);
                }
            }

            let buffer = self.buffer_pool.offset((written_pages * BLCKSZ) as isize);
            if let Some(file) = self.current_file.as_mut() {
                let slice =
                    std::slice::from_raw_parts(buffer as *const u8, (BLCKSZ * flush_num) as usize);
                file.write_all(slice).unwrap();
            }

            written_pages += flush_num;
        }

        self.total_blks += num_pages;
        self.close_rel_file();

        if !(*relation).rd_smgr.is_null() {
            (*(*relation).rd_smgr).smgr_cached_nblocks[MAIN_FORKNUM as usize] =
                self.current_blk_num();
        }
        pg_sys::CacheInvalidateRelcache(relation);

        self.current_page = 0;
        Self::init_page(self.get_current_page());
    }

    fn current_blk_num(&self) -> BlockNumber {
        self.existing_blks + self.total_blks
    }

    unsafe fn init_page(page: Page) {
        pg_sys::PageInit(page, BLCKSZ as usize, 0);
    }

    fn get_current_page(&self) -> Page {
        self.get_target_page(self.current_page)
    }

    fn get_target_page(&self, blk_num: BlockNumber) -> Page {
        unsafe { self.buffer_pool.offset((blk_num * BLCKSZ) as isize) }
    }
}

impl TpchTuple for Nation<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.n_nationkey.into_datum().unwrap();
        datums[1] = crate::encoding::text_datum_str(self.n_name);
        datums[2] = self.n_regionkey.into_datum().unwrap();
        datums[3] = crate::encoding::text_datum_str(self.n_comment);
    }
}

impl TpchTuple for Region<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.r_regionkey.into_datum().unwrap();
        datums[1] = crate::encoding::text_datum_str(self.r_name);
        datums[2] = crate::encoding::text_datum_str(self.r_comment);
    }
}

impl TpchTuple for Part<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.p_partkey.into_datum().unwrap();
        datums[1] = crate::encoding::text_datum(&self.p_name);
        datums[2] = crate::encoding::text_datum(&self.p_mfgr);
        datums[3] = crate::encoding::text_datum(&self.p_brand);
        datums[4] = crate::encoding::text_datum_str(self.p_type);
        datums[5] = self.p_size.into_datum().unwrap();
        datums[6] = crate::encoding::text_datum_str(self.p_container);
        datums[7] = crate::encoding::decimal_numeric_datum(self.p_retailprice);
        datums[8] = crate::encoding::text_datum_str(self.p_comment);
    }
}

impl TpchTuple for Supplier {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.s_suppkey.into_datum().unwrap();
        datums[1] = crate::encoding::text_datum(&self.s_name);
        datums[2] = crate::encoding::text_datum(&self.s_address);
        datums[3] = self.s_nationkey.into_datum().unwrap();
        datums[4] = crate::encoding::text_datum(&self.s_phone);
        datums[5] = crate::encoding::decimal_numeric_datum(self.s_acctbal);
        datums[6] = crate::encoding::text_datum_str(self.s_comment.as_str());
    }
}

impl TpchTuple for Customer<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.c_custkey.into_datum().unwrap();
        datums[1] = crate::encoding::text_datum(&self.c_name);
        datums[2] = crate::encoding::text_datum(&self.c_address);
        datums[3] = self.c_nationkey.into_datum().unwrap();
        datums[4] = crate::encoding::text_datum(&self.c_phone);
        datums[5] = crate::encoding::decimal_numeric_datum(self.c_acctbal);
        datums[6] = crate::encoding::text_datum_str(self.c_mktsegment);
        datums[7] = crate::encoding::text_datum_str(self.c_comment);
    }
}

impl TpchTuple for PartSupp<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.ps_partkey.into_datum().unwrap();
        datums[1] = self.ps_suppkey.into_datum().unwrap();
        datums[2] = self.ps_availqty.into_datum().unwrap();
        datums[3] = crate::encoding::decimal_numeric_datum(self.ps_supplycost);
        datums[4] = crate::encoding::text_datum_str(self.ps_comment);
    }
}

impl TpchTuple for Order<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.o_orderkey.into_datum().unwrap();
        datums[1] = self.o_custkey.into_datum().unwrap();
        datums[2] = crate::encoding::text_datum_str(self.o_orderstatus.as_str());
        datums[3] = crate::encoding::decimal_numeric_datum(self.o_totalprice);
        datums[4] = crate::encoding::date_datum(self.o_orderdate);
        datums[5] = crate::encoding::text_datum_str(self.o_orderpriority);
        datums[6] = crate::encoding::text_datum(self.o_clerk);
        datums[7] = self.o_shippriority.into_datum().unwrap();
        datums[8] = crate::encoding::text_datum_str(self.o_comment);
    }
}

impl TpchTuple for LineItem<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.l_orderkey.into_datum().unwrap();
        datums[1] = self.l_partkey.into_datum().unwrap();
        datums[2] = self.l_suppkey.into_datum().unwrap();
        datums[3] = self.l_linenumber.into_datum().unwrap();
        datums[4] = crate::encoding::integer_numeric_datum(self.l_quantity);
        datums[5] = crate::encoding::decimal_numeric_datum(self.l_extendedprice);
        datums[6] = crate::encoding::decimal_numeric_datum(self.l_discount);
        datums[7] = crate::encoding::decimal_numeric_datum(self.l_tax);
        datums[8] = crate::encoding::text_datum_str(self.l_returnflag);
        datums[9] = crate::encoding::text_datum_str(self.l_linestatus);
        datums[10] = crate::encoding::date_datum(self.l_shipdate);
        datums[11] = crate::encoding::date_datum(self.l_commitdate);
        datums[12] = crate::encoding::date_datum(self.l_receiptdate);
        datums[13] = crate::encoding::text_datum_str(self.l_shipinstruct);
        datums[14] = crate::encoding::text_datum_str(self.l_shipmode);
        datums[15] = crate::encoding::text_datum_str(self.l_comment);
    }
}
