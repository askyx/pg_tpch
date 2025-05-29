#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>

#include <format>
#include <map>
#include <string>
#include <utility>
#include <vector>

#define DECLARER

#include "dbgen_gunk.hpp"
#include "tpch_dsdgen.h"

extern "C" {
#include <c.h>
#include <postgres.h>
#include <fmgr.h>

#include <access/htup.h>
#include <access/table.h>
#include <access/xloginsert.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <nodes/print.h>
#include <storage/block.h>
#include <storage/bufmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>

#include "utils/palloc.h"
}

#include <generator>

#include "dss.h"
#include "dsstypes.h"

namespace tpch {

template <typename DSSType, typename MKRetType, typename... Args>
void call_dbgen_mk(size_t idx, DSSType& value, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, DBGenContext* ctx, Args...),
                   int dbgen_table_id, DBGenContext* ctx, Args... args) {
  row_start(dbgen_table_id, ctx);

  mk_fn(idx, &value, ctx, std::forward<Args>(args)...);

  row_stop_h(dbgen_table_id, ctx);
}

void skip(int table, int children, DSS_HUGE step, DBGenContext& dbgen_ctx) {
  switch (table) {
    case CUST:
      sd_cust(children, step, &dbgen_ctx);
      break;
    case SUPP:
      sd_supp(children, step, &dbgen_ctx);
      break;
    case NATION:
      sd_nation(children, step, &dbgen_ctx);
      break;
    case REGION:
      sd_region(children, step, &dbgen_ctx);
      break;
    case ORDER_LINE:
    case LINE:
    case ORDER:
      sd_line(children, step, &dbgen_ctx);
      sd_order(children, step, &dbgen_ctx);
      break;
    case PART_PSUPP:
    case PART:
    case PSUPP:
      sd_part(children, step, &dbgen_ctx);
      sd_psupp(children, step, &dbgen_ctx);
      break;
  }
}

std::string convert_money_str(DSS_HUGE cents) {
  if (cents < 0) {
    cents = std::abs(cents);
    return std::format("-{}.{:02d}", cents / 100, cents % 100);
  } else
    return std::format("{}.{:02d}", cents / 100, cents % 100);
}

std::string convert_str(auto date) {
  return std::format("{}", date);
}

struct LoaderInfo {
  TupleDesc desc;
  DBGenContext* generate_ctx;
  size_t rowcnt;
  size_t offset;

  int current_item = 0;
  std::vector<FmgrInfo> in_functions;
  std::vector<Oid> typioparams;

  LoaderInfo(TupleDesc desc, DBGenContext* generate_ctx, size_t rowcnt, size_t offset)
      : desc(desc), generate_ctx(generate_ctx), rowcnt(rowcnt), offset(offset) {
    Oid in_func_oid;

    in_functions.resize(desc->natts);
    typioparams.resize(desc->natts);
    for (auto attnum = 1; attnum <= desc->natts; attnum++) {
      Form_pg_attribute att = TupleDescAttr(desc, attnum - 1);

      getTypeInputInfo(att->atttypid, &in_func_oid, &typioparams[attnum - 1]);
      fmgr_info(in_func_oid, &in_functions[attnum - 1]);
    }
  }

  void reset() { current_item = 0; }

  template <typename T>
  Datum addItem(T value) {
    Datum v;
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*> || std::is_same_v<T, char>)
      v = DirectFunctionCall3(in_functions[current_item].fn_addr, CStringGetDatum(value),
                              ObjectIdGetDatum(typioparams[current_item]),
                              TupleDescAttr(desc, current_item)->atttypmod);
    else  // else check
      v = value;

    current_item++;
    return v;
  }

  std::generator<HeapTuple> generate(int table_id) {
    if (table_id == NATION)
      return generate_nation();
    else if (table_id == CUST)
      return generate_customer();
    else if (table_id == ORDER)
      return generate_orders();
    else if (table_id == LINE)
      return generate_lineitem();
    else if (table_id == PART)
      return generate_part();
    else if (table_id == PSUPP)
      return generate_partsupp();
    else if (table_id == REGION)
      return generate_region();
    else if (table_id == SUPP)
      return generate_supplier();

    std::unreachable();
  }

  std::generator<HeapTuple> generate_supplier() {
    supplier_t supplier{};
    Datum values[7] = {0};
    bool nulls[7] = {false};

    for (auto supplier_idx = offset; rowcnt; rowcnt--, ++supplier_idx) {
      call_dbgen_mk<supplier_t>(supplier_idx + 1, supplier, mk_supp, SUPP, generate_ctx);

      values[0] = addItem(supplier.suppkey);
      values[1] = addItem(supplier.name);
      values[2] = addItem(supplier.address);
      values[3] = addItem(supplier.nation_code);
      values[4] = addItem(supplier.phone);
      values[5] = addItem(convert_money_str(supplier.acctbal).data());
      values[6] = addItem(supplier.comment);

      reset();
      co_yield heap_form_tuple(desc, values, nulls);
    }
  }

  std::generator<HeapTuple> generate_region() {
    code_t region{};
    Datum values[3] = {0};
    bool nulls[3] = {false};

    for (auto region_idx = offset; rowcnt; rowcnt--, ++region_idx) {
      call_dbgen_mk<code_t>(region_idx + 1, region, mk_region, REGION, generate_ctx);
      values[0] = addItem(region.code);
      values[1] = addItem(region.text);
      values[2] = addItem(region.comment);

      reset();
      co_yield heap_form_tuple(desc, values, nulls);
    }
  }

  std::generator<HeapTuple> generate_partsupp() {
    partsupp__t partsupps{};
    Datum values[5] = {0};
    bool nulls[5] = {false};

    for (auto part_idx = offset; rowcnt; rowcnt--, ++part_idx) {
      call_dbgen_mk<partsupp__t>(part_idx + 1, partsupps, mk_partsupp, PART_PSUPP, generate_ctx);

      for (auto partsupp : partsupps.s) {
        values[0] = addItem(partsupp.partkey);
        values[1] = addItem(partsupp.suppkey);
        values[2] = addItem(partsupp.qty);
        values[3] = addItem(convert_money_str(partsupp.scost).data());
        values[4] = addItem(partsupp.comment);

        reset();
        co_yield heap_form_tuple(desc, values, nulls);
      }
    }
  }

  std::generator<HeapTuple> generate_part() {
    part_t part{};
    Datum values[9] = {0};
    bool nulls[9] = {false};

    for (auto part_idx = offset; rowcnt; rowcnt--, ++part_idx) {
      call_dbgen_mk<part_t>(part_idx + 1, part, mk_part, PART_PSUPP, generate_ctx);

      values[0] = addItem(part.partkey);
      values[1] = addItem(part.name);
      values[2] = addItem(part.mfgr);
      values[3] = addItem(part.brand);
      values[4] = addItem(part.type);
      values[5] = addItem(part.size);
      values[6] = addItem(part.container);
      values[7] = addItem(convert_money_str(part.retailprice).data());
      values[8] = addItem(part.comment);

      reset();
      co_yield heap_form_tuple(desc, values, nulls);
    }
  }

  std::generator<HeapTuple> generate_lineitem() {
    order_t order{};
    Datum values[16] = {0};
    bool nulls[16] = {false};

    for (auto order_idx = offset; rowcnt; rowcnt--, ++order_idx) {
      call_dbgen_mk<order_t>(order_idx + 1, order, mk_lineitem, ORDER_LINE, generate_ctx, 0l);

      for (auto line_idx = int64_t{0}; line_idx < order.lines; ++line_idx) {
        const auto& lineitem = order.l[line_idx];

        values[0] = addItem(lineitem.okey);
        values[1] = addItem(lineitem.partkey);
        values[2] = addItem(lineitem.suppkey);
        values[3] = addItem(lineitem.lcnt);
        values[4] = addItem(convert_money_str(lineitem.quantity).data());
        values[5] = addItem(convert_money_str(lineitem.eprice).data());
        values[6] = addItem(convert_money_str(lineitem.discount).data());
        values[7] = addItem(convert_money_str(lineitem.tax).data());
        values[8] = addItem(convert_str(lineitem.rflag[0]).data());
        values[9] = addItem(convert_str(lineitem.lstatus[0]).data());
        values[10] = addItem(lineitem.sdate);
        values[11] = addItem(lineitem.cdate);
        values[12] = addItem(lineitem.rdate);
        values[13] = addItem(lineitem.shipinstruct);
        values[14] = addItem(lineitem.shipmode);
        values[15] = addItem(lineitem.comment);
        reset();

        co_yield heap_form_tuple(desc, values, nulls);
      }
    }
  }

  std::generator<HeapTuple> generate_orders() {
    order_t order{};
    Datum values[9] = {0};
    bool nulls[9] = {false};

    for (auto order_idx = offset; rowcnt > 0; rowcnt--, ++order_idx) {
      call_dbgen_mk<order_t>(order_idx + 1, order, mk_order, ORDER_LINE, generate_ctx, 0l);

      values[0] = addItem(order.okey);
      values[1] = addItem(order.custkey);
      values[2] = addItem(convert_str(order.orderstatus).data());
      values[3] = addItem(convert_money_str(order.totalprice).data());
      values[4] = addItem(order.odate);
      values[5] = addItem(order.opriority);
      values[6] = addItem(order.clerk);
      values[7] = addItem(order.spriority);
      values[8] = addItem(order.comment);
      reset();

      co_yield heap_form_tuple(desc, values, nulls);
    }
  }

  std::generator<HeapTuple> generate_customer() {
    customer_t customer{};
    Datum values[8] = {0};
    bool nulls[8] = {false};

    for (auto row_idx = offset; rowcnt > 0; rowcnt--, row_idx++) {
      call_dbgen_mk<customer_t>(row_idx + 1, customer, mk_cust, CUST, generate_ctx);

      values[0] = addItem(customer.custkey);
      values[1] = addItem(customer.name);
      values[2] = addItem(customer.address);
      values[3] = addItem(customer.nation_code);
      values[4] = addItem(customer.phone);
      values[5] = addItem(convert_money_str(customer.acctbal).data());
      values[6] = addItem(customer.mktsegment);
      values[7] = addItem(customer.comment);
      reset();

      co_yield heap_form_tuple(desc, values, nulls);
    }
  }

  std::generator<HeapTuple> generate_nation() {
    code_t nation{};
    Datum values[4] = {0};
    bool nulls[4] = {false};

    for (auto nation_idx = offset; rowcnt > 0; rowcnt--, ++nation_idx) {
      call_dbgen_mk<code_t>(nation_idx + 1, nation, mk_nation, NATION, generate_ctx);
      values[0] = addItem(nation.code);
      values[1] = addItem(nation.text);
      values[2] = addItem(nation.join);
      values[3] = addItem(nation.comment);
      reset();

      co_yield heap_form_tuple(desc, values, nulls);
    }
  }
};

struct TpchXX {
  static constexpr auto BUFFER_POOL_SIZE = 1024;

  char* buffer_pool;
  BlockNumber current_page{0};  // current processing page in buffer pool
  BlockNumber total_pages{0};   // all processed pages, include flushed page
  BlockNumber existing_pages;   // alreay existing pages in table

  Relation relation;

  TransactionId xid;
  CommandId cid;

  File opend_file{-1};

  TpchXX(char* table) {
    auto reloid = DirectFunctionCall1(regclassin, CStringGetDatum(table));
    relation = try_table_open(reloid, NoLock);
    if (!relation)
      throw std::runtime_error("try_table_open Failed");

    xid = GetCurrentTransactionId();
    cid = GetCurrentCommandId(true);
    existing_pages = RelationGetNumberOfBlocks(relation);
    buffer_pool = (char*)palloc(BLCKSZ * BUFFER_POOL_SIZE);

    init_current_page();
  }

  ~TpchXX() {
    flush_page();
    if (pg_fsync(opend_file) != 0)
      ereport(WARNING, (errcode_for_file_access(), errmsg("could not sync data file: %m")));
    if (close(opend_file) < 0)
      ereport(WARNING, (errcode_for_file_access(), errmsg("could not close data file: %m")));
    opend_file = -1;
    pfree(buffer_pool);
    table_close(relation, NoLock);
  }

  int load(DBGenContext* ctx, int table, size_t row_count, size_t offset) {
    LoaderInfo loader{RelationGetDescr(relation), ctx, row_count, offset};
    int count = 0;
    for (auto&& tuple : loader.generate(table)) {
      input(tuple);
      count++;
    }
    return count;
  }

  Page get_current_page() { return buffer_pool + BLCKSZ * current_page; }

  BlockNumber get_current_page_number() { return total_pages + existing_pages; }

  void close_or_open_file() {
    auto current_page = get_current_page_number();
    if (current_page % RELSEG_SIZE == 0 && opend_file != -1) {
      if (pg_fsync(opend_file) != 0)
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not sync data file: %m")));
      if (close(opend_file) < 0)
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not close data file: %m")));
      opend_file = -1;
    }

    if (opend_file == -1) {
      RelFileLocatorBackend file_loc{relation->rd_locator, (ProcNumber)InvalidCommandId};
      auto fname = relpath(file_loc, MAIN_FORKNUM);
      if (auto segno = current_page / RELSEG_SIZE; segno > 0) {
        char* tmp = (char*)palloc(strlen(fname) + 12);

        sprintf(tmp, "%s.%u", fname, segno);
        pfree(fname);
        fname = tmp;
      }

      if (opend_file = BasicOpenFilePerm(fname, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR); opend_file < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", fname)));
      }
      if (auto x = lseek(opend_file, BLCKSZ * (current_page % RELSEG_SIZE), SEEK_SET); x < 0) {
        close(opend_file);
        opend_file = -1;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek to position %u in file \"%s\": %m",
                                                          BLCKSZ * (current_page % RELSEG_SIZE), fname)));
      }
      pfree(fname);
    }
  }

  void flush_page() {
    auto flush_page = current_page;
    if (!PageIsEmpty(get_current_page()))
      flush_page += 1;

    if (flush_page > 0) {
      if (unlikely(total_pages == 0)) {
        auto recptr = log_newpage(&relation->rd_locator, MAIN_FORKNUM, existing_pages, buffer_pool, true);
        XLogFlush(recptr);
      }

      for (auto i = 0; i < flush_page;) {
        auto current_page = get_current_page_number();

        close_or_open_file();

        auto sflush_page = std::min(RELSEG_SIZE - (current_page % RELSEG_SIZE), flush_page - i);

        auto buffer_pos = buffer_pool + BLCKSZ * i;
        auto buffer_w = BLCKSZ * sflush_page;
        while (buffer_w > 0) {
          if (auto written = write(opend_file, buffer_pos, buffer_w); written == -1)
            throw std::runtime_error("write failed");
          else {
            buffer_pos += written;
            buffer_w -= written;
          }
        }
        i += sflush_page;
      }

      total_pages += flush_page;
    }
  }

  void init_current_page() {
    auto page = get_current_page();

    PageInit(page, BLCKSZ, 0);
  }

  void fix_tuple(HeapTuple tuple) {
    tuple->t_data->t_infomask &= ~(HEAP_XACT_MASK);
    tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
    tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
    HeapTupleHeaderSetXmin(tuple->t_data, xid);
    HeapTupleHeaderSetCmin(tuple->t_data, cid);
    HeapTupleHeaderSetXmax(tuple->t_data, 0);
  }

  void input(HeapTuple tuple) {
    auto page = get_current_page();
    if (PageGetFreeSpace(page) <
        MAXALIGN(tuple->t_len) + RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR)) {
      if (current_page < BUFFER_POOL_SIZE - 1) {
        current_page++;
      } else {
        flush_page();
        current_page = 0;
      }
      init_current_page();
      page = get_current_page();
    }

    fix_tuple(tuple);

    auto offnum = PageAddItem(page, (Item)tuple->t_data, tuple->t_len, InvalidOffsetNumber, false, true);

    ItemPointerSet(&(tuple->t_self), get_current_page_number() + current_page, offnum);
    auto item_id = PageGetItemId(page, offnum);
    auto item = PageGetItem(page, item_id);
    ((HeapTupleHeader)item)->t_ctid = tuple->t_self;
  }
};

TPCHTableGenerator::TPCHTableGenerator(double scale_factor) {
  tdef* tdefs = ctx_.tdefs;
  tdefs[PART].base = 200000;
  tdefs[PSUPP].base = 200000;
  tdefs[SUPP].base = 10000;
  tdefs[CUST].base = 150000;
  tdefs[ORDER].base = 150000 * ORDERS_PER_CUST;
  tdefs[LINE].base = 150000 * ORDERS_PER_CUST;
  tdefs[ORDER_LINE].base = 150000 * ORDERS_PER_CUST;
  tdefs[PART_PSUPP].base = 200000;
  tdefs[NATION].base = NATIONS_MAX;
  tdefs[REGION].base = NATIONS_MAX;

  if (scale_factor < MIN_SCALE) {
    int i;
    int int_scale;

    ctx_.scale_factor = 1;
    int_scale = (int)(1000 * scale_factor);
    for (i = PART; i < REGION; i++) {
      tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
      if (ctx_.tdefs[i].base < 1) {
        tdefs[i].base = 1;
      }
    }
  } else {
    ctx_.scale_factor = (long)scale_factor;
  }
  load_dists(10 * 1024 * 1024, &ctx_);  // 10MiB
  tdefs[NATION].base = nations.count;
  tdefs[REGION].base = regions.count;
}

TPCHTableGenerator::~TPCHTableGenerator() {
  cleanup_dists();
}

int TPCHTableGenerator::load_table(char* table, int children, int step) {
  std::map<std::string, int> table_mape = {
      {"nation", NATION}, {"region", REGION}, {"part", PART},    {"partsupp", PSUPP},
      {"supplier", SUPP}, {"customer", CUST}, {"orders", ORDER}, {"lineitem", LINE},
  };

  if (table_mape.find(table) == table_mape.end())
    throw std::runtime_error("Invalid table name");

  auto table_id = table_mape[table];

  size_t rowcnt = 0;
  int offset = 0;

  if (table_id < NATION)
    rowcnt = ctx_.tdefs[table_id].base * ctx_.scale_factor;
  else
    rowcnt = ctx_.tdefs[table_id].base;

  if (children > 1 && step != -1) {
    size_t part_size = std::ceil((double)rowcnt / (double)children);
    offset = part_size * step;
    auto part_end = offset + part_size;
    rowcnt = part_end > rowcnt ? rowcnt - offset : part_size;
    skip(table_id, children, offset, ctx_);
  }

  TpchXX tpch_nation(table);
  return tpch_nation.load(&ctx_, table_id, rowcnt, offset);
}

}  // namespace tpch
