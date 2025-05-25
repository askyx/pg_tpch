use std::{
    cmp::min,
    io::{Seek, SeekFrom, Write},
};

use pgrx::{
    datum::Date,
    direct_function_call,
    pg_sys::{
        self, BlockNumber, ForkNumber::MAIN_FORKNUM, HeapTuple, Page, Pointer, BLCKSZ, RELSEG_SIZE,
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
    fn into_datums(self) -> Vec<pg_sys::Datum>;
}

pub(crate) fn load_rows<R, I>(table: &str, rows: I) -> i64
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
}

impl Loader {
    pub fn new(table: &str) -> Self {
        let relation = Self::open_target_relation(table);
        Self::validate_target_relation(&relation);
        let has_indexes = Self::relation_has_indexes(&relation);

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

    pub fn generate_rows_and_load<R, I>(&mut self, rows: I) -> i64
    where
        R: TpchTuple,
        I: IntoIterator<Item = R>,
    {
        let mut inserted = 0_i64;
        unsafe {
            for row in rows {
                self.load_row(row);
                inserted += 1;
            }
            self.flush();
        }

        if self.has_indexes {
            self.reindex_target_relation();
        }

        inserted
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
        let tuple = Self::tuple_from_row(&self.relation, row);
        self.load_tuple(tuple);
    }

    fn tuple_from_row<R: TpchTuple>(rel: &PgRelation, row: R) -> HeapTuple {
        let mut datums = row.into_datums();
        let mut nulls = vec![false; datums.len()];

        unsafe {
            pg_sys::heap_form_tuple(
                rel.tuple_desc().as_ptr(),
                datums.as_mut_ptr(),
                nulls.as_mut_ptr(),
            )
        }
    }

    unsafe fn load_tuple(&mut self, tuple: HeapTuple) {
        let mut page = self.get_current_page();
        if pg_sys::PageGetFreeSpace(page)
            < (std::mem::size_of::<pg_sys::ItemIdData>()
                + pg_sys::MAXALIGN((*tuple).t_len as usize)
                + crate::utils::PGUtils::relation_get_target_page_free_space(
                    &self.relation,
                    pg_sys::HEAP_DEFAULT_FILLFACTOR,
                ))
        {
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

        crate::utils::PGUtils::heap_tuple_header_set_xmin(
            values,
            pg_sys::GetCurrentTransactionId(),
        );
        crate::utils::PGUtils::heap_tuple_header_set_cmin(
            values,
            pg_sys::GetCurrentCommandId(true),
        );
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
    }

    unsafe fn flush(&mut self) {
        let mut num_pages = self.current_page;
        if !pg_sys::PageIsEmpty(self.get_current_page()) {
            num_pages += 1;
        }

        if num_pages == 0 {
            return;
        }

        if self.total_blks == 0 {
            let relation = self.relation.as_ptr();
            let recptr = pg_sys::log_newpage(
                &mut (*relation).rd_locator as *mut pg_sys::RelFileLocator,
                MAIN_FORKNUM,
                self.existing_blks,
                self.get_current_page(),
                true,
            );
            pg_sys::XLogFlush(recptr);
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

        let relation = self.relation.as_ptr();
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
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.n_nationkey.into_datum().unwrap(),
            text_datum(self.n_name),
            self.n_regionkey.into_datum().unwrap(),
            text_datum(self.n_comment),
        ]
    }
}

impl TpchTuple for Region<'_> {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.r_regionkey.into_datum().unwrap(),
            text_datum(self.r_name),
            text_datum(self.r_comment),
        ]
    }
}

impl TpchTuple for Part<'_> {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.p_partkey.into_datum().unwrap(),
            text_datum(self.p_name),
            text_datum(self.p_mfgr),
            text_datum(self.p_brand),
            text_datum(self.p_type),
            self.p_size.into_datum().unwrap(),
            text_datum(self.p_container),
            decimal_datum(self.p_retailprice),
            text_datum(self.p_comment),
        ]
    }
}

impl TpchTuple for Supplier {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.s_suppkey.into_datum().unwrap(),
            text_datum(self.s_name),
            text_datum(self.s_address),
            self.s_nationkey.into_datum().unwrap(),
            text_datum(self.s_phone),
            decimal_datum(self.s_acctbal),
            text_datum(self.s_comment),
        ]
    }
}

impl TpchTuple for Customer<'_> {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.c_custkey.into_datum().unwrap(),
            text_datum(self.c_name),
            text_datum(self.c_address),
            self.c_nationkey.into_datum().unwrap(),
            text_datum(self.c_phone),
            decimal_datum(self.c_acctbal),
            text_datum(self.c_mktsegment),
            text_datum(self.c_comment),
        ]
    }
}

impl TpchTuple for PartSupp<'_> {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.ps_partkey.into_datum().unwrap(),
            self.ps_suppkey.into_datum().unwrap(),
            self.ps_availqty.into_datum().unwrap(),
            decimal_datum(self.ps_supplycost),
            text_datum(self.ps_comment),
        ]
    }
}

impl TpchTuple for Order<'_> {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.o_orderkey.into_datum().unwrap(),
            self.o_custkey.into_datum().unwrap(),
            text_datum(self.o_orderstatus.as_str()),
            decimal_datum(self.o_totalprice),
            date_datum(self.o_orderdate),
            text_datum(self.o_orderpriority),
            text_datum(self.o_clerk),
            self.o_shippriority.into_datum().unwrap(),
            text_datum(self.o_comment),
        ]
    }
}

impl TpchTuple for LineItem<'_> {
    fn into_datums(self) -> Vec<pg_sys::Datum> {
        vec![
            self.l_orderkey.into_datum().unwrap(),
            self.l_partkey.into_datum().unwrap(),
            self.l_suppkey.into_datum().unwrap(),
            self.l_linenumber.into_datum().unwrap(),
            numeric_datum(self.l_quantity),
            decimal_datum(self.l_extendedprice),
            decimal_datum(self.l_discount),
            decimal_datum(self.l_tax),
            text_datum(self.l_returnflag),
            text_datum(self.l_linestatus),
            date_datum(self.l_shipdate),
            date_datum(self.l_commitdate),
            date_datum(self.l_receiptdate),
            text_datum(self.l_shipinstruct),
            text_datum(self.l_shipmode),
            text_datum(self.l_comment),
        ]
    }
}
