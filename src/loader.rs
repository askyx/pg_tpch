use std::{
    cmp::min,
    io::{Seek, SeekFrom, Write},
    time::Instant,
};

use pgrx::{
    datum::Date,
    direct_function_call,
    pg_sys::{
        self, BlockNumber, ForkNumber::MAIN_FORKNUM, HeapTuple, MemoryContext, Page, Pointer,
        BLCKSZ, RELSEG_SIZE,
    },
    AnyNumeric, IntoDatum, PgList, PgRelation, Spi,
};
use tpchgen::{
    dates::TPCHDate,
    decimal::TPCHDecimal,
    generators::{Customer, LineItem, Nation, Order, Part, PartSupp, Region, Supplier},
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

pub(crate) struct Loader {
    buffer_pool: Pointer,
    current_page: BlockNumber,
    total_blks: BlockNumber,
    existing_blks: BlockNumber,
    relation: PgRelation,
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
            buffer_pool: unsafe { pg_sys::palloc((BLCKSZ * MAX_BLOCK_NUMBER) as usize) as Pointer },
            current_page: 0,
            total_blks: 0,
            existing_blks: unsafe {
                pg_sys::RelationGetNumberOfBlocksInFork(relation.as_ptr(), MAIN_FORKNUM)
            },
            relation,
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
            for row in rows {
                self.load_row(row);
                inserted += 1;
            }
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
        Spi::run(&format!("REINDEX TABLE {qualified_name}")).unwrap();
    }

    fn close_rel_file(&mut self) {
        if let Some(mut f) = self.current_file.take() {
            f.flush().unwrap();
            std::mem::drop(f);
        }
    }

    fn open_rel_file(&mut self, blk_num: BlockNumber) {
        let locator = (*self.relation).rd_locator;

        let fname = unsafe {
            pg_sys::GetRelationPath(
                locator.dbOid,
                locator.spcOid,
                locator.relNumber,
                pg_sys::INVALID_PROC_NUMBER as i32,
                MAIN_FORKNUM,
            )
        };

        let mut path = unsafe {
            core::ffi::CStr::from_ptr(fname.str_.as_ptr())
                .to_string_lossy()
                .into_owned()
        };

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
        let old_ctx = pg_sys::MemoryContextSwitchTo(self.memctx);

        row.write_datums(&mut self.datums);
        for n in &mut self.nulls {
            *n = false;
        }

        let tuple = pg_sys::heap_form_tuple(
            self.relation.tuple_desc().as_ptr(),
            self.datums.as_mut_ptr(),
            self.nulls.as_mut_ptr(),
        );

        let switched = self.load_tuple(tuple);
        pg_sys::heap_freetuple(tuple);

        pg_sys::MemoryContextSwitchTo(old_ctx);

        if switched {
            pg_sys::MemoryContextReset(self.memctx);
        }
    }

    unsafe fn load_tuple(&mut self, tuple: HeapTuple) -> bool {
        let mut page = self.get_current_page();
        let tuple_size = std::mem::size_of::<pg_sys::ItemIdData>()
            + pg_sys::MAXALIGN((*tuple).t_len as usize)
            + crate::utils::PGUtils::relation_get_target_page_free_space(
                &self.relation,
                pg_sys::HEAP_DEFAULT_FILLFACTOR,
            );

        let mut switched = false;
        if pg_sys::PageGetFreeSpace(page) < tuple_size {
            switched = true;
            if self.current_page < MAX_BLOCK_NUMBER - 1 {
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
            if pg_sys::DataChecksumsEnabled() {
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

fn text_datum(value: impl ToString) -> pg_sys::Datum {
    value.to_string().into_datum().unwrap()
}

fn decimal_datum(value: TPCHDecimal) -> pg_sys::Datum {
    let numeric = AnyNumeric::try_from(value.to_string().as_str()).unwrap();
    numeric.into_datum().unwrap()
}

fn numeric_datum(value: impl ToString) -> pg_sys::Datum {
    let numeric = AnyNumeric::try_from(value.to_string().as_str()).unwrap();
    numeric.into_datum().unwrap()
}

fn date_datum(value: TPCHDate) -> pg_sys::Datum {
    let (year, month, day) = value.to_ymd();
    Date::new(1900 + year, month as u8, day as u8)
        .unwrap()
        .into_datum()
        .unwrap()
}

impl TpchTuple for Nation<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.n_nationkey.into_datum().unwrap();
        datums[1] = text_datum(self.n_name);
        datums[2] = self.n_regionkey.into_datum().unwrap();
        datums[3] = text_datum(self.n_comment);
    }
}

impl TpchTuple for Region<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.r_regionkey.into_datum().unwrap();
        datums[1] = text_datum(self.r_name);
        datums[2] = text_datum(self.r_comment);
    }
}

impl TpchTuple for Part<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.p_partkey.into_datum().unwrap();
        datums[1] = text_datum(&self.p_name);
        datums[2] = text_datum(&self.p_mfgr);
        datums[3] = text_datum(&self.p_brand);
        datums[4] = text_datum(&self.p_type);
        datums[5] = self.p_size.into_datum().unwrap();
        datums[6] = text_datum(&self.p_container);
        datums[7] = decimal_datum(self.p_retailprice);
        datums[8] = text_datum(&self.p_comment);
    }
}

impl TpchTuple for Supplier {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.s_suppkey.into_datum().unwrap();
        datums[1] = text_datum(&self.s_name);
        datums[2] = text_datum(&self.s_address);
        datums[3] = self.s_nationkey.into_datum().unwrap();
        datums[4] = text_datum(&self.s_phone);
        datums[5] = decimal_datum(self.s_acctbal);
        datums[6] = text_datum(&self.s_comment);
    }
}

impl TpchTuple for Customer<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.c_custkey.into_datum().unwrap();
        datums[1] = text_datum(&self.c_name);
        datums[2] = text_datum(&self.c_address);
        datums[3] = self.c_nationkey.into_datum().unwrap();
        datums[4] = text_datum(&self.c_phone);
        datums[5] = decimal_datum(self.c_acctbal);
        datums[6] = text_datum(&self.c_mktsegment);
        datums[7] = text_datum(&self.c_comment);
    }
}

impl TpchTuple for PartSupp<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.ps_partkey.into_datum().unwrap();
        datums[1] = self.ps_suppkey.into_datum().unwrap();
        datums[2] = self.ps_availqty.into_datum().unwrap();
        datums[3] = decimal_datum(self.ps_supplycost);
        datums[4] = text_datum(self.ps_comment);
    }
}

impl TpchTuple for Order<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.o_orderkey.into_datum().unwrap();
        datums[1] = self.o_custkey.into_datum().unwrap();
        datums[2] = text_datum(self.o_orderstatus.as_str());
        datums[3] = decimal_datum(self.o_totalprice);
        datums[4] = date_datum(self.o_orderdate);
        datums[5] = text_datum(self.o_orderpriority);
        datums[6] = text_datum(self.o_clerk);
        datums[7] = self.o_shippriority.into_datum().unwrap();
        datums[8] = text_datum(self.o_comment);
    }
}

impl TpchTuple for LineItem<'_> {
    fn write_datums(&self, datums: &mut [pg_sys::Datum]) {
        datums[0] = self.l_orderkey.into_datum().unwrap();
        datums[1] = self.l_partkey.into_datum().unwrap();
        datums[2] = self.l_suppkey.into_datum().unwrap();
        datums[3] = self.l_linenumber.into_datum().unwrap();
        datums[4] = numeric_datum(self.l_quantity);
        datums[5] = decimal_datum(self.l_extendedprice);
        datums[6] = decimal_datum(self.l_discount);
        datums[7] = decimal_datum(self.l_tax);
        datums[8] = text_datum(self.l_returnflag);
        datums[9] = text_datum(self.l_linestatus);
        datums[10] = date_datum(self.l_shipdate);
        datums[11] = date_datum(self.l_commitdate);
        datums[12] = date_datum(self.l_receiptdate);
        datums[13] = text_datum(self.l_shipinstruct);
        datums[14] = text_datum(self.l_shipmode);
        datums[15] = text_datum(self.l_comment);
    }
}
