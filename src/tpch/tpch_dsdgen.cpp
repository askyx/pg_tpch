#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <format>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#define DECLARER

#include "dbgen_gunk.hpp"
#include "tpch_constants.hpp"
#include "tpch_dsdgen.h"

extern "C" {
#include <postgres.h>

#include <access/table.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
}

#include "dss.h"
#include "dsstypes.h"

namespace tpch {

const std::unordered_map<TPCHTable, std::underlying_type_t<TPCHTable>> tpch_table_to_dbgen_id = {
    {TPCHTable::Part, PART},    {TPCHTable::PartSupp, PSUPP}, {TPCHTable::Supplier, SUPP}, {TPCHTable::Customer, CUST},
    {TPCHTable::Orders, ORDER}, {TPCHTable::LineItem, LINE},  {TPCHTable::Nation, NATION}, {TPCHTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
void call_dbgen_mk(size_t idx, DSSType& value, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, DBGenContext* ctx, Args...),
                   TPCHTable table, DBGenContext* ctx, Args... args) {
  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id, ctx);

  mk_fn(idx, &value, ctx, std::forward<Args>(args)...);

  row_stop_h(dbgen_table_id, ctx);
}

TPCHTableGenerator::TPCHTableGenerator(double scale_factor, const std::string& table,
                                       std::filesystem::path resource_dir, int rng_seed)
    : table_{std::move(table)} {
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

class TableLoader {
 public:
  TableLoader(const std::string& table) : table_{std::move(table)} {
    reloid_ = DirectFunctionCall1(regclassin, CStringGetDatum(table_.c_str()));
    rel_ = try_table_open(reloid_, RowExclusiveLock);
    if (!rel_)
      throw std::runtime_error("try_table_open Failed");

    auto tupDesc = RelationGetDescr(rel_);
    Oid in_func_oid;

    in_functions = new FmgrInfo[tupDesc->natts];
    typioparams = new Oid[tupDesc->natts];

    for (auto attnum = 1; attnum <= tupDesc->natts; attnum++) {
      Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

      getTypeInputInfo(att->atttypid, &in_func_oid, &typioparams[attnum - 1]);
      fmgr_info(in_func_oid, &in_functions[attnum - 1]);
    }

    slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsMinimalTuple);
    slot->tts_tableOid = RelationGetRelid(rel_);
  };

  ~TableLoader() {
    table_close(rel_, RowExclusiveLock);
    free(in_functions);
    free(typioparams);
    ExecDropSingleTupleTableSlot(slot);
  }

  template <typename T>
  auto& addItem(T value) {
    Datum datum;
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*> || std::is_same_v<T, char>)
      slot->tts_values[current_item_] = DirectFunctionCall3(
          in_functions[current_item_].fn_addr, CStringGetDatum(value), ObjectIdGetDatum(typioparams[current_item_]),
          TupleDescAttr(RelationGetDescr(rel_), current_item_)->atttypmod);
    else  // else check
      slot->tts_values[current_item_] = value;

    current_item_++;
    return *this;
  }

  auto& start() {
    ExecClearTuple(slot);
    MemSet(slot->tts_values, 0, RelationGetDescr(rel_)->natts * sizeof(Datum));
    /* all tpch table is not null */
    MemSet(slot->tts_isnull, false, RelationGetDescr(rel_)->natts * sizeof(bool));
    current_item_ = 0;
    return *this;
  }

  auto& end() {
    ExecStoreVirtualTuple(slot);

    table_tuple_insert(rel_, slot, mycid, ti_options, NULL);
    // reindex ？
    // if (rel_->ri_NumIndices > 0)
    //   recheckIndexes = ExecInsertIndexTuples(rel_, myslot, estate, false, false, NULL, NIL, false);

    row_count_++;
    return *this;
  }

  auto row_count() const { return row_count_; }

  Oid reloid_;
  Relation rel_;
  std::string table_;
  size_t row_count_ = 0;
  size_t current_item_ = 0;

  FmgrInfo* in_functions;
  Oid* typioparams;
  TupleTableSlot* slot;
  CommandId mycid = GetCurrentCommandId(true);
  int ti_options = 0;
};

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

int TPCHTableGenerator::generate_customer() {
  const auto customer_count = static_cast<uint32_t>(ctx_.tdefs[CUST].base * ctx_.scale_factor);

  TableLoader loader(table_);

  customer_t customer{};
  for (auto row_idx = size_t{0}; row_idx < customer_count; row_idx++) {
    call_dbgen_mk<customer_t>(row_idx + 1, customer, mk_cust, TPCHTable::Customer, &ctx_);

    loader.start()
        .addItem(customer.custkey)
        .addItem(customer.name)
        .addItem(customer.address)
        .addItem(customer.nation_code)
        .addItem(customer.phone)
        .addItem(convert_money_str(customer.acctbal).data())
        .addItem(customer.mktsegment)
        .addItem(customer.comment)
        .end();
  }

  return loader.row_count();
}

std::pair<int, int> TPCHTableGenerator::generate_orders_and_lineitem() {
  const auto order_count = static_cast<size_t>(ctx_.tdefs[ORDER].base * ctx_.scale_factor);

  TableLoader order_loader("orders");
  TableLoader lineitem_loader("lineitem");

  order_t order{};
  for (auto order_idx = size_t{0}; order_idx < order_count; ++order_idx) {
    call_dbgen_mk<order_t>(order_idx + 1, order, mk_order, TPCHTable::Orders, &ctx_, 0l);

    order_loader.start()
        .addItem(order.okey)
        .addItem(order.custkey)
        .addItem(convert_str(order.orderstatus).data())
        .addItem(convert_money_str(order.totalprice).data())
        .addItem(order.odate)
        .addItem(order.opriority)
        .addItem(order.clerk)
        .addItem(order.spriority)
        .addItem(order.comment)
        .end();

    for (auto line_idx = int64_t{0}; line_idx < order.lines; ++line_idx) {
      const auto& lineitem = order.l[line_idx];

      lineitem_loader.start()
          .addItem(lineitem.okey)
          .addItem(lineitem.partkey)
          .addItem(lineitem.suppkey)
          .addItem(lineitem.lcnt)
          .addItem(convert_money_str(lineitem.quantity).data())
          .addItem(convert_money_str(lineitem.eprice).data())
          .addItem(convert_money_str(lineitem.discount).data())
          .addItem(convert_money_str(lineitem.tax).data())
          .addItem(convert_str(lineitem.rflag[0]).data())
          .addItem(convert_str(lineitem.lstatus[0]).data())
          .addItem(lineitem.sdate)
          .addItem(lineitem.cdate)
          .addItem(lineitem.rdate)
          .addItem(lineitem.shipinstruct)
          .addItem(lineitem.shipmode)
          .addItem(lineitem.comment)
          .end();
    }
  }

  return {order_loader.row_count(), lineitem_loader.row_count()};
}

int TPCHTableGenerator::generate_nation() {
  const auto nation_count = static_cast<size_t>(ctx_.tdefs[NATION].base);

  TableLoader loader(table_);

  code_t nation{};
  for (auto nation_idx = size_t{0}; nation_idx < nation_count; ++nation_idx) {
    call_dbgen_mk<code_t>(nation_idx + 1, nation, mk_nation, TPCHTable::Nation, &ctx_);
    loader.start().addItem(nation.code).addItem(nation.text).addItem(nation.join).addItem(nation.comment).end();
  }

  return loader.row_count();
}

std::pair<int, int> TPCHTableGenerator::generate_part_and_partsupp() {
  const auto part_count = static_cast<size_t>(ctx_.tdefs[PART].base * ctx_.scale_factor);

  TableLoader part_loader("part");
  TableLoader partsupp_loader("partsupp");

  part_t part{};
  for (auto part_idx = size_t{0}; part_idx < part_count; ++part_idx) {
    call_dbgen_mk<part_t>(part_idx + 1, part, mk_part, TPCHTable::Part, &ctx_);

    part_loader.start()
        .addItem(part.partkey)
        .addItem(part.name)
        .addItem(part.mfgr)
        .addItem(part.brand)
        .addItem(part.type)
        .addItem(part.size)
        .addItem(part.container)
        .addItem(convert_money_str(part.retailprice).data())
        .addItem(part.comment)
        .end();

    for (const auto& partsupp : part.s) {
      partsupp_loader.start()
          .addItem(partsupp.partkey)
          .addItem(partsupp.suppkey)
          .addItem(partsupp.qty)
          .addItem(convert_money_str(partsupp.scost).data())
          .addItem(partsupp.comment)
          .end();
    }
  }

  return {part_loader.row_count(), partsupp_loader.row_count()};
}

int TPCHTableGenerator::generate_region() {
  const auto region_count = static_cast<size_t>(ctx_.tdefs[REGION].base);

  TableLoader loader(table_);

  code_t region{};
  for (auto region_idx = size_t{0}; region_idx < region_count; ++region_idx) {
    call_dbgen_mk<code_t>(region_idx + 1, region, mk_region, TPCHTable::Region, &ctx_);
    loader.start().addItem(region.code).addItem(region.text).addItem(region.comment).end();
  }

  return loader.row_count();
}

int TPCHTableGenerator::generate_supplier() {
  const auto supplier_count = static_cast<size_t>(ctx_.tdefs[SUPP].base * ctx_.scale_factor);

  TableLoader loader(table_);

  supplier_t supplier{};
  for (auto supplier_idx = size_t{0}; supplier_idx < supplier_count; ++supplier_idx) {
    call_dbgen_mk<supplier_t>(supplier_idx + 1, supplier, mk_supp, TPCHTable::Supplier, &ctx_);

    loader.start()
        .addItem(supplier.suppkey)
        .addItem(supplier.name)
        .addItem(supplier.address)
        .addItem(supplier.nation_code)
        .addItem(supplier.phone)
        .addItem(convert_money_str(supplier.acctbal).data())
        .addItem(supplier.comment)
        .end();
  }

  return loader.row_count();
}

}  // namespace tpch
