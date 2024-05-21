#include "tpch_dsdgen.h"

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

extern "C" {
#include <postgres.h>

#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include "dss.h"
#include "dsstypes.h"
#include "tpch_dbgen.h"
}

namespace tpch {

const std::unordered_map<TPCHTable, std::underlying_type_t<TPCHTable>> tpch_table_to_dbgen_id = {
    {TPCHTable::Part, PART},    {TPCHTable::PartSupp, PSUPP}, {TPCHTable::Supplier, SUPP}, {TPCHTable::Customer, CUST},
    {TPCHTable::Orders, ORDER}, {TPCHTable::LineItem, LINE},  {TPCHTable::Nation, NATION}, {TPCHTable::Region, REGION}};

template <typename DSSType, typename MKRetType, typename... Args>
DSSType call_dbgen_mk(size_t idx, MKRetType (*mk_fn)(DSS_HUGE, DSSType* val, Args...), TPCHTable table, Args... args) {
  /**
   * Preserve calling scheme (row_start(); mk...(); row_stop(); as in dbgen's gen_tbl())
   */

  const auto dbgen_table_id = tpch_table_to_dbgen_id.at(table);

  row_start(dbgen_table_id);

  DSSType value{};
  mk_fn(idx, &value, std::forward<Args>(args)...);

  row_stop(dbgen_table_id);

  return value;
}

TPCHTableGenerator::TPCHTableGenerator(uint32_t scale_factor, const std::string& table, int max_row,
                                       std::filesystem::path resource_dir, int rng_seed)
    : table_{std::move(table)}, _scale_factor{scale_factor}, max_row_{max_row} {
  dbgen_reset_seeds();
  dbgen_init_scale_factor(_scale_factor);
  // atexit([]() { throw std::runtime_error("TPCHTableGenerator internal error"); });
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

  // ugly code
  template <typename T>
  auto& addItem(const std::optional<T>& value) {
    curr_cid_++;

    std::string pos;
    if (curr_cid_ < col_size_)
      pos = ",";
    else
      pos = "";

    if (value.has_value()) {
      if constexpr (std::same_as<T, std::string>)
        sql += std::format("'{}'{}", value.value(), pos);
      else
        sql += std::format("{}{}", value.value(), pos);
    } else
      sql += std::format("null{}", pos);

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

    if constexpr (std::is_same_v<T, char*>)
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

int TPCHTableGenerator::generate_customer() const {
  const auto customer_count = static_cast<uint32_t>(tdefs[CUST].base * scale);

  TableLoader loader(table_, 18, 100);

  for (auto row_idx = size_t{0}; row_idx < customer_count; row_idx++) {
    auto customer = call_dbgen_mk<customer_t>(row_idx + 1, mk_cust, TPCHTable::Customer);

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

int TPCHTableGenerator::generate_orders_and_lineitem() const {
  const auto order_count = static_cast<ChunkOffset>(tdefs[ORDER].base * scale);

  TableLoader order_loader(table_, 18, 100);
  TableLoader lineitem_loader(table_, 18, 100);

  for (auto order_idx = size_t{0}; order_idx < order_count; ++order_idx) {
    const auto order = call_dbgen_mk<order_t>(order_idx + 1, mk_order, TPCHTable::Orders, 0l);

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

int TPCHTableGenerator::generate_nation() const {
  const auto nation_count = static_cast<ChunkOffset>(tdefs[NATION].base);

  TableLoader loader(table_, 28, 100);

  for (auto nation_idx = size_t{0}; nation_idx < nation_count; ++nation_idx) {
    const auto nation = call_dbgen_mk<code_t>(nation_idx + 1, mk_nation, TPCHTable::Nation);
    loader.start().addItem(nation.code).addItem(nation.text).addItem(nation.join).addItem(nation.comment).end();
  }

  return loader.row_count();
}

int TPCHTableGenerator::generate_part_and_partsupp() const {
  const auto part_count = static_cast<ChunkOffset>(tdefs[PART].base * scale);

  TableLoader part_loader(table_, 5, 100);
  TableLoader partsupp_loader(table_, 5, 100);

  for (auto part_idx = size_t{0}; part_idx < part_count; ++part_idx) {
    const auto part = call_dbgen_mk<part_t>(part_idx + 1, mk_part, TPCHTable::Part);

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

    // Some scale factors (e.g., 0.05) are not supported by tpch-dbgen as they produce non-unique partkey/suppkey
    // combinations. The reason is probably somewhere in the magic in PART_SUPP_BRIDGE. As the partkey is
    // ascending, those are easy to identify:

    DSS_HUGE last_partkey = {};
    auto suppkeys = std::vector<DSS_HUGE>{};

    for (const auto& partsupp : part.s) {
      {
        // Make sure we do not generate non-unique combinations (see above)
        if (partsupp.partkey != last_partkey) {
          // Assert(partsupp.partkey > last_partkey, "Expected partkey to be generated in ascending order.");
          last_partkey = partsupp.partkey;
          suppkeys.clear();
        }
        // Assert(std::find(suppkeys.begin(), suppkeys.end(), partsupp.suppkey) == suppkeys.end(),
        //        "Scale factor unsupported by tpch-dbgen. Consider choosing a \"round\" number.");
        suppkeys.emplace_back(partsupp.suppkey);
      }

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

int TPCHTableGenerator::generate_region() const {
  const auto region_count = static_cast<ChunkOffset>(tdefs[REGION].base);

  TableLoader loader(table_, 3, 100);

  for (auto region_idx = size_t{0}; region_idx < region_count; ++region_idx) {
    const auto region = call_dbgen_mk<code_t>(region_idx + 1, mk_region, TPCHTable::Region);
    loader.start().addItem(region.code).addItem(region.text).addItem(region.comment).end();
  }

  return loader.row_count();
}

int TPCHTableGenerator::generate_supplier() const {
  const auto supplier_count = static_cast<ChunkOffset>(tdefs[SUPP].base * scale);

  TableLoader loader(table_, 4, 100);

  for (auto supplier_idx = size_t{0}; supplier_idx < supplier_count; ++supplier_idx) {
    const auto supplier = call_dbgen_mk<supplier_t>(supplier_idx + 1, mk_supp, TPCHTable::Supplier);

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
