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

#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
}

#include "dss.h"
#include "dsstypes.h"

namespace tpch {

const std::unordered_map<TPCHTable, std::underlying_type_t<TPCHTable>> tpch_table_to_dbgen_id = {
    {TPCHTable::Part, PART},    {TPCHTable::PartSupp, PSUPP}, {TPCHTable::Supplier, SUPP}, {TPCHTable::Customer, CUST},
    {TPCHTable::Orders, ORDER}, {TPCHTable::LineItem, LINE},  {TPCHTable::Nation, NATION}, {TPCHTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, DBGenContext* ctx, Args...),
                      TPCHTable table, DBGenContext* ctx, Args... args) {
  /**
   * Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())
   */

  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id, ctx);

  DSSType value{};
  mk_fn(idx, &value, ctx, std::forward<Args>(args)...);

  row_stop_h(dbgen_table_id, ctx);

  return value;
}

TPCHTableGenerator::TPCHTableGenerator(uint32_t scale_factor, const std::string& table, int max_row,
                                       std::filesystem::path resource_dir, int rng_seed)
    : table_{std::move(table)}, _scale_factor{scale_factor}, max_row_{max_row} {
  // atexit([]() { throw std::runtime_error("TPCHTableGenerator internal error"); });
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

  if (_scale_factor < MIN_SCALE) {
    int i;
    int int_scale;

    ctx_.scale_factor = 1;
    int_scale = (int)(1000 * _scale_factor);
    for (i = PART; i < REGION; i++) {
      tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
      if (ctx_.tdefs[i].base < 1) {
        tdefs[i].base = 1;
      }
    }
  } else {
    ctx_.scale_factor = (long)_scale_factor;
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
  TableLoader(const std::string& table, size_t col_size, size_t batch_size)
      : table_{std::move(table)}, col_size_(col_size), batch_size_(batch_size) {
    if (SPI_connect() != SPI_OK_CONNECT)
      throw std::runtime_error("SPI_connect Failed");
  };

  ~TableLoader() {
    if (curr_batch_ != 0) {
      auto insert = std::format("INSERT INTO {} VALUES {}", table_, sql);
      SPI_exec(insert.c_str(), 0);
    }
    SPI_finish();
  }

  auto& start() {
    curr_batch_++;
    row_count_++;
    if (curr_batch_ < batch_size_) {
      if (curr_batch_ != 1)
        sql += ",";
    } else {
      // do insert
      auto insert = std::format("INSERT INTO {} VALUES {}", table_, sql);
      auto result = SPI_exec(insert.c_str(), 0);
      if (result != SPI_OK_INSERT) {
        throw std::runtime_error("TPCHTableGenerator internal error");
      }
      sql.clear();
      curr_batch_ = 1;
    }
    sql += "(";

    return *this;
  }

  template <typename T>
    requires(std::is_trivial_v<T>)
  auto& addItem(T value) {
    curr_cid_++;

    std::string pos;
    if (curr_cid_ < col_size_)
      pos = ",";
    else
      pos = "";

    if constexpr (std::is_same_v<std::remove_cv_t<T>, char*> || std::is_same_v<std::remove_cv_t<T>, const char*> ||
                  std::is_same_v<std::remove_cv_t<T>, char> || std::is_same_v<std::remove_cv_t<T>, const char>)
      sql += std::format("'{}'{}", value, pos);
    else
      sql += std::format("{}{}", value, pos);

    return *this;
  }

  auto& end() {
    sql += ")";
    if (col_size_ != curr_cid_)
      throw std::runtime_error("TPCHTableGenerator internal error");
    curr_cid_ = 0;

    return *this;
  }

  auto row_count() const { return row_count_; }

 private:
  std::string table_;
  std::string sql;
  size_t col_size_;
  size_t curr_cid_ = 0;
  size_t batch_size_;
  size_t curr_batch_ = 0;
  size_t row_count_ = 0;
};

float convert_money(DSS_HUGE cents) {
  const auto dollars = cents / 100;
  cents %= 100;
  return static_cast<float>(dollars) + (static_cast<float>(cents)) / 100.0f;
}

int TPCHTableGenerator::generate_customer() {
  const auto customer_count = static_cast<uint32_t>(ctx_.tdefs[CUST].base * ctx_.scale_factor);

  TableLoader loader(table_, 8, 100);

  for (auto row_idx = size_t{0}; row_idx < customer_count; row_idx++) {
    auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TPCHTable::Customer, &ctx_);

    loader.start()
        .addItem(customer.custkey)
        .addItem(customer.name)
        .addItem(customer.address)
        .addItem(customer.nation_code)
        .addItem(customer.phone)
        .addItem(convert_money(customer.acctbal))
        .addItem(customer.mktsegment)
        .addItem(customer.comment)
        .end();
  }

  return loader.row_count();
}

int TPCHTableGenerator::generate_orders_and_lineitem() {
  const auto order_count = static_cast<size_t>(ctx_.tdefs[ORDER].base * ctx_.scale_factor);

  TableLoader order_loader("orders", 9, 100);
  TableLoader lineitem_loader("lineitem", 16, 100);

  for (auto order_idx = size_t{0}; order_idx < order_count; ++order_idx) {
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TPCHTable::Orders, &ctx_, 0l);

    order_loader.start()
        .addItem(order.okey)
        .addItem(order.custkey)
        .addItem(order.orderstatus)
        .addItem(convert_money(order.totalprice))
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
          .addItem(lineitem.quantity)
          .addItem(convert_money(lineitem.eprice))
          .addItem(convert_money(lineitem.discount))
          .addItem(convert_money(lineitem.tax))
          .addItem(lineitem.rflag[0])
          .addItem(lineitem.lstatus[0])
          .addItem(lineitem.sdate)
          .addItem(lineitem.cdate)
          .addItem(lineitem.rdate)
          .addItem(lineitem.shipinstruct)
          .addItem(lineitem.shipmode)
          .addItem(lineitem.comment)
          .end();
    }
  }

  return std::max(order_loader.row_count(), lineitem_loader.row_count());
}

int TPCHTableGenerator::generate_nation() {
  const auto nation_count = static_cast<size_t>(ctx_.tdefs[NATION].base);

  TableLoader loader(table_, 4, 100);

  for (auto nation_idx = size_t{0}; nation_idx < nation_count; ++nation_idx) {
    const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TPCHTable::Nation, &ctx_);
    loader.start().addItem(nation.code).addItem(nation.text).addItem(nation.join).addItem(nation.comment).end();
  }

  return loader.row_count();
}

int TPCHTableGenerator::generate_part_and_partsupp() {
  const auto part_count = static_cast<size_t>(ctx_.tdefs[PART].base * ctx_.scale_factor);

  TableLoader part_loader("part", 9, 100);
  TableLoader partsupp_loader("partsupp", 5, 100);

  for (auto part_idx = size_t{0}; part_idx < part_count; ++part_idx) {
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TPCHTable::Part, &ctx_);

    part_loader.start()
        .addItem(part.partkey)
        .addItem(part.name)
        .addItem(part.mfgr)
        .addItem(part.brand)
        .addItem(part.type)
        .addItem(part.size)
        .addItem(part.container)
        .addItem(convert_money(part.retailprice))
        .addItem(part.comment)
        .end();

    for (const auto& partsupp : part.s) {
      partsupp_loader.start()
          .addItem(partsupp.partkey)
          .addItem(partsupp.suppkey)
          .addItem(partsupp.qty)
          .addItem(convert_money(partsupp.scost))
          .addItem(partsupp.comment)
          .end();
    }
  }

  return std::max(part_loader.row_count(), partsupp_loader.row_count());
}

int TPCHTableGenerator::generate_region() {
  const auto region_count = static_cast<size_t>(ctx_.tdefs[REGION].base);

  TableLoader loader(table_, 3, 100);

  for (auto region_idx = size_t{0}; region_idx < region_count; ++region_idx) {
    const auto region = call_dbgen_mk<code_t>(region_idx + 1, mk_region, TPCHTable::Region, &ctx_);
    loader.start().addItem(region.code).addItem(region.text).addItem(region.comment).end();
  }

  return loader.row_count();
}

int TPCHTableGenerator::generate_supplier() {
  const auto supplier_count = static_cast<size_t>(ctx_.tdefs[SUPP].base * ctx_.scale_factor);

  TableLoader loader(table_, 7, 100);

  for (auto supplier_idx = size_t{0}; supplier_idx < supplier_count; ++supplier_idx) {
    const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TPCHTable::Supplier, &ctx_);

    loader.start()
        .addItem(supplier.suppkey)
        .addItem(supplier.name)
        .addItem(supplier.address)
        .addItem(supplier.nation_code)
        .addItem(supplier.phone)
        .addItem(convert_money(supplier.acctbal))
        .addItem(supplier.comment)
        .end();
  }

  return loader.row_count();
}

}  // namespace tpch
