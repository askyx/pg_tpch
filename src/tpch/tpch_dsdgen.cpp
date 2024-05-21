#include "tpcds_dsdgen.h"

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

// TODO split pg functions into other file

#ifdef snprintf
#undef snprintf
#endif

#include "tpcds-kit/tools/address.h"
#include "tpcds-kit/tools/columns.h"
#include "tpcds-kit/tools/date.h"
#include "tpcds-kit/tools/decimal.h"
#include "tpcds-kit/tools/dist.h"
#include "tpcds-kit/tools/genrand.h"
#include "tpcds-kit/tools/nulls.h"
#include "tpcds-kit/tools/parallel.h"
#include "tpcds-kit/tools/porting.h"
#include "tpcds-kit/tools/r_params.h"
#include "tpcds-kit/tools/tables.h"
#include "tpcds-kit/tools/tdefs.h"
#include "tpcds-kit/tools/w_call_center.h"
#include "tpcds-kit/tools/w_catalog_page.h"
#include "tpcds-kit/tools/w_catalog_returns.h"
#include "tpcds-kit/tools/w_catalog_sales.h"
#include "tpcds-kit/tools/w_customer.h"
#include "tpcds-kit/tools/w_customer_address.h"
#include "tpcds-kit/tools/w_customer_demographics.h"
#include "tpcds-kit/tools/w_datetbl.h"
#include "tpcds-kit/tools/w_household_demographics.h"
#include "tpcds-kit/tools/w_income_band.h"
#include "tpcds-kit/tools/w_inventory.h"
#include "tpcds-kit/tools/w_item.h"
#include "tpcds-kit/tools/w_promotion.h"
#include "tpcds-kit/tools/w_reason.h"
#include "tpcds-kit/tools/w_ship_mode.h"
#include "tpcds-kit/tools/w_store.h"
#include "tpcds-kit/tools/w_store_returns.h"
#include "tpcds-kit/tools/w_store_sales.h"
#include "tpcds-kit/tools/w_timetbl.h"
#include "tpcds-kit/tools/w_warehouse.h"
#include "tpcds-kit/tools/w_web_page.h"
#include "tpcds-kit/tools/w_web_returns.h"
#include "tpcds-kit/tools/w_web_sales.h"
#include "tpcds-kit/tools/w_web_site.h"
}

namespace tpcds {
void init_tpcds_tools(uint32_t scale_factor, std::string resource_dir, int rng_seed) {
  // setting some values that were intended by dsdgen to be passed via command
  // line

  auto scale_factor_string = std::string{"SCALE"};
  auto scale_factor_value_string = std::to_string(scale_factor);
  set_int(scale_factor_string.data(), scale_factor_value_string.data());

  auto rng_seed_string = std::string{"RNGSEED"};
  auto rng_seed_value_string = std::to_string(rng_seed);
  set_int(rng_seed_string.data(), rng_seed_value_string.data());

  // init_rand from genrand.c, adapted
  {
    const auto n_seed = get_int(rng_seed_string.data());
    const auto skip = INT_MAX / MAX_COLUMN;
    for (auto index = 0; index < MAX_COLUMN; ++index) {
      const auto seed = n_seed + skip * index;
      Streams[index].nInitialSeed = seed;
      Streams[index].nSeed = seed;
      Streams[index].nUsed = 0;
    }
  }

  mk_w_store_sales_master(nullptr, 0, 1);
  mk_w_web_sales_master(nullptr, 0, 1);
  mk_w_web_sales_detail(nullptr, 0, nullptr, nullptr, 1);
  mk_w_catalog_sales_master(nullptr, 0, 1);

  auto distributions_string = std::string{"DISTRIBUTIONS"};
  auto distributions_value = resource_dir + "/tpcds.idx";
  set_str(distributions_string.data(), distributions_value.data());

  for (auto table_id = int{0}; table_id <= MAX_TABLE; ++table_id) {
    resetSeeds(table_id);
    RNGReset(table_id);
  }
}

template <class TpcdsRow, int builder(void*, ds_key_t), int table_id>
TpcdsRow call_dbgen_mk(ds_key_t index) {
  auto tpcds_row = TpcdsRow{};
  builder(&tpcds_row, index);
  tpcds_row_stop(table_id);
  return tpcds_row;
}

// get starting index and row count for a table, see
// third_party/tpcds-kit/tools/driver.c:549
std::pair<ds_key_t, ds_key_t> prepare_for_table(int table_id) {
  auto k_row_count = ds_key_t{};
  auto k_first_row = ds_key_t{};

  split_work(table_id, &k_first_row, &k_row_count);

  const auto& tdefs = *getSimpleTdefsByNumber(table_id);

  if (k_first_row != 1) {
    row_skip(table_id, static_cast<int>(k_first_row - 1));
    if (tdefs.flags & FL_PARENT) {  // NOLINT
      row_skip(tdefs.nParam, static_cast<int>(k_first_row - 1));
    }
  }

  if (tdefs.flags & FL_SMALL) {  // NOLINT
    resetCountCount();
  }

  // Assert(k_row_count <= std::numeric_limits<tpcds_key_t>::max(),
  //        "tpcds_key_t is too small for this scale factor, "
  //        "consider using tpcds_key_t = int64_t;");
  return {k_first_row, k_row_count};
}

std::string boolean_to_string(bool boolean) {
  return {boolean ? "Y" : "N"};
}

std::string zip_to_string(int32_t zip) {
  auto result = std::string(5, '?');
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  const auto snprintf_rc = std::snprintf(result.data(), result.size() + 1, "%05d", zip);
  // Assert(snprintf_rc > 0, "Unexpected string to parse.");
  return result;
}

TPCDSTableGenerator::TPCDSTableGenerator(uint32_t scale_factor, const std::string& table, int max_row,
                                         std::filesystem::path resource_dir, int rng_seed)
    : table_{std::move(table)}, _scale_factor{scale_factor}, max_row_{max_row} {
  init_tpcds_tools(scale_factor, resource_dir.string(), rng_seed);
  // atexit([]() { throw std::runtime_error("TPCDSTableGenerator internal error"); });
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
        throw std::runtime_error("TPCDSTableGenerator internal error");
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
      throw std::runtime_error("TPCDSTableGenerator internal error");
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

// dsdgen deliberately creates NULL values if nullCheck(column_id) is true,
// resolve functions mimic that
std::optional<std::string> resolve_date_id(int column_id, ds_key_t date_id) {
  if (nullCheck(column_id) != 0 || date_id <= 0) {
    return std::nullopt;
  }

  auto date = date_t{};
  jtodt(&date, static_cast<int>(date_id));

  auto result = std::string(10, '?');
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  const auto snprintf_rc =
      std::snprintf(result.data(), result.size() + 1, "%4d-%02d-%02d", date.year, date.month, date.day);
  // NOLINTEND(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  // Assert(snprintf_rc > 0, "Unexpected string to parse.");

  return result;
}

using tpcds_key_t = int32_t;

std::optional<tpcds_key_t> resolve_key(int column_id, ds_key_t key) {
  return nullCheck(column_id) != 0 || key == -1 ? std::nullopt : std::optional{static_cast<tpcds_key_t>(key)};
}

std::optional<std::string> resolve_string(int column_id, std::string string) {
  if (nullCheck(column_id) != 0 || string.empty())
    return std::nullopt;

  std::string str;
  std::ranges::for_each(string, [&](char& c) {
    if (c == '\'')
      str += "''";
    else
      str += c;
  });
  return str;
}

std::optional<int32_t> resolve_integer(int column_id, int value) {
  return nullCheck(column_id) != 0 ? std::nullopt : std::optional{int32_t{value}};
}

std::optional<std::string> resolve_street_name(int column_id, const ds_addr_t& address) {
  return nullCheck(column_id) != 0 ? std::nullopt
         : address.street_name2 == nullptr
             ? std::optional{std::string{address.street_name1}}
             : std::optional{std::string{address.street_name1} + " " + address.street_name2};
}

std::optional<float> resolve_gmt_offset(int column_id, int32_t gmt_offset) {
  return nullCheck(column_id) != 0 ? std::nullopt : std::optional{static_cast<float>(gmt_offset)};
}

std::optional<float> resolve_decimal(int column_id, decimal_t decimal) {
  auto result = 0.0;
  dectof(&result, &decimal);
  // we have to divide by 10 after dectof to get the expected result
  return nullCheck(column_id) != 0 ? std::nullopt : std::optional{static_cast<float>(result / 10)};
}

int TPCDSTableGenerator::generate_call_center() const {
  auto [call_center_first, call_center_count] = prepare_for_table(CALL_CENTER);

  auto call_center = CALL_CENTER_TBL{};

  TableLoader loader(table_, 31, 100);
  call_center.cc_closed_date_id = ds_key_t{-1};
  for (auto call_center_index = ds_key_t{0}; call_center_index < call_center_count; ++call_center_index) {
    // mk_w_call_center needs a pointer to the previous result of
    // mk_w_call_center to add "update entries" for the same call center
    mk_w_call_center(&call_center, call_center_first + call_center_index);
    loader.start()
        .addItem(call_center.cc_call_center_sk)
        .addItem(call_center.cc_call_center_id)
        .addItem(resolve_date_id(CC_REC_START_DATE_ID, call_center.cc_rec_start_date_id))
        .addItem(resolve_date_id(CC_REC_END_DATE_ID, call_center.cc_rec_end_date_id))
        .addItem(resolve_key(CC_CLOSED_DATE_ID, call_center.cc_closed_date_id))
        .addItem(resolve_key(CC_OPEN_DATE_ID, call_center.cc_open_date_id))
        .addItem(resolve_string(CC_NAME, call_center.cc_name))
        .addItem(resolve_string(CC_CLASS, call_center.cc_class))
        .addItem(resolve_integer(CC_EMPLOYEES, call_center.cc_employees))
        .addItem(resolve_integer(CC_SQ_FT, call_center.cc_sq_ft))
        .addItem(resolve_string(CC_HOURS, call_center.cc_hours))
        .addItem(resolve_string(CC_MANAGER, call_center.cc_manager))
        .addItem(resolve_integer(CC_MARKET_ID, call_center.cc_market_id))
        .addItem(resolve_string(CC_MARKET_CLASS, call_center.cc_market_class))
        .addItem(resolve_string(CC_MARKET_DESC, call_center.cc_market_desc))
        .addItem(resolve_string(CC_MARKET_MANAGER, call_center.cc_market_manager))
        .addItem(resolve_integer(CC_DIVISION, call_center.cc_division_id))
        .addItem(resolve_string(CC_DIVISION_NAME, call_center.cc_division_name))
        .addItem(resolve_integer(CC_COMPANY, call_center.cc_company))
        .addItem(resolve_string(CC_COMPANY_NAME, call_center.cc_company_name))
        .addItem(resolve_string(CC_ADDRESS, std::to_string(call_center.cc_address.street_num)))
        .addItem(resolve_street_name(CC_ADDRESS, call_center.cc_address))
        .addItem(resolve_string(CC_ADDRESS, call_center.cc_address.street_type))
        .addItem(resolve_string(CC_ADDRESS, call_center.cc_address.suite_num))
        .addItem(resolve_string(CC_ADDRESS, call_center.cc_address.city))
        .addItem(resolve_string(CC_ADDRESS, call_center.cc_address.county))
        .addItem(resolve_string(CC_ADDRESS, call_center.cc_address.state))
        .addItem(resolve_string(CC_ADDRESS, zip_to_string(call_center.cc_address.zip)))
        .addItem(resolve_string(CC_ADDRESS, call_center.cc_address.country))
        .addItem(resolve_gmt_offset(CC_ADDRESS, call_center.cc_address.gmt_offset))
        .addItem(resolve_decimal(CC_TAX_PERCENTAGE, call_center.cc_tax_percentage))
        .end();
    tpcds_row_stop(CALL_CENTER);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_catalog_page() const {
  auto [catalog_page_first, catalog_page_count] = prepare_for_table(CATALOG_PAGE);

  auto catalog_page = CATALOG_PAGE_TBL{};
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  const auto snprintf_rc =
      std::snprintf(catalog_page.cp_department, sizeof(catalog_page.cp_department), "%s", "DEPARTMENT");
  // NOLINTEND(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  // Assert(snprintf_rc > 0, "Unexpected string to parse.");
  TableLoader loader(table_, 9, 100);

  for (auto catalog_page_index = ds_key_t{0}; catalog_page_index < catalog_page_count; ++catalog_page_index) {
    // need a pointer to the previous result of mk_w_catalog_page, because
    // cp_department is only set once
    mk_w_catalog_page(&catalog_page, catalog_page_first + catalog_page_index);
    loader.start()
        .addItem(catalog_page.cp_catalog_page_sk)
        .addItem(catalog_page.cp_catalog_page_id)
        .addItem(resolve_key(CP_START_DATE_ID, catalog_page.cp_start_date_id))
        .addItem(resolve_key(CP_END_DATE_ID, catalog_page.cp_end_date_id))
        .addItem(resolve_string(CP_DEPARTMENT, catalog_page.cp_department))
        .addItem(resolve_integer(CP_CATALOG_NUMBER, catalog_page.cp_catalog_number))
        .addItem(resolve_integer(CP_CATALOG_PAGE_NUMBER, catalog_page.cp_catalog_page_number))
        .addItem(resolve_string(CP_DESCRIPTION, catalog_page.cp_description))
        .addItem(resolve_string(CP_TYPE, catalog_page.cp_type))
        .end();

    tpcds_row_stop(CATALOG_PAGE);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_catalog_sales_and_returns() const {
  auto [catalog_sales_first, catalog_sales_count] = prepare_for_table(CATALOG_SALES);
  // catalog_sales_count is NOT the actual number of catalog sales created, for each of these "master" catalog_sales
  // multiple "detail" catalog sales are created and possibly returned

  TableLoader loader_catalog_sales("catalog_sales", 34, 100);
  TableLoader loader_catalog_returns("catalog_returns", 27, 100);

  for (auto catalog_sale_index = ds_key_t{0}; catalog_sale_index < catalog_sales_count; ++catalog_sale_index) {
    auto catalog_sales = W_CATALOG_SALES_TBL{};
    auto catalog_returns = W_CATALOG_RETURNS_TBL{};

    // modified call to mk_w_catalog_sales(&catalog_sales, catalog_sales_first + catalog_sale_index,
    //                                     &catalog_returns, &was_returned);
    {
      mk_w_catalog_sales_master(&catalog_sales, catalog_sales_first + catalog_sale_index, 0);
      auto n_lineitems = int{0};
      genrand_integer(&n_lineitems, DIST_UNIFORM, 4, 14, 0, CS_ORDER_NUMBER);
      for (auto lineitem_index = int{0}; lineitem_index < n_lineitems; ++lineitem_index) {
        auto was_returned = int{0};
        mk_w_catalog_sales_detail(&catalog_sales, 0, &catalog_returns, &was_returned);

        if (loader_catalog_sales.row_count() < max_row_) {
          loader_catalog_sales.start()
              .addItem(resolve_key(CS_SOLD_DATE_SK, catalog_sales.cs_sold_date_sk))
              .addItem(resolve_key(CS_SOLD_TIME_SK, catalog_sales.cs_sold_time_sk))
              .addItem(resolve_key(CS_SHIP_DATE_SK, catalog_sales.cs_ship_date_sk))
              .addItem(resolve_key(CS_BILL_CUSTOMER_SK, catalog_sales.cs_bill_customer_sk))
              .addItem(resolve_key(CS_BILL_CDEMO_SK, catalog_sales.cs_bill_cdemo_sk))
              .addItem(resolve_key(CS_BILL_HDEMO_SK, catalog_sales.cs_bill_hdemo_sk))
              .addItem(resolve_key(CS_BILL_ADDR_SK, catalog_sales.cs_bill_addr_sk))
              .addItem(resolve_key(CS_SHIP_CUSTOMER_SK, catalog_sales.cs_ship_customer_sk))
              .addItem(resolve_key(CS_SHIP_CDEMO_SK, catalog_sales.cs_ship_cdemo_sk))
              .addItem(resolve_key(CS_SHIP_HDEMO_SK, catalog_sales.cs_ship_hdemo_sk))
              .addItem(resolve_key(CS_SHIP_ADDR_SK, catalog_sales.cs_ship_addr_sk))
              .addItem(resolve_key(CS_CALL_CENTER_SK, catalog_sales.cs_call_center_sk))
              .addItem(resolve_key(CS_CATALOG_PAGE_SK, catalog_sales.cs_catalog_page_sk))
              .addItem(resolve_key(CS_SHIP_MODE_SK, catalog_sales.cs_ship_mode_sk))
              .addItem(resolve_key(CS_WAREHOUSE_SK, catalog_sales.cs_warehouse_sk))
              .addItem(catalog_sales.cs_sold_item_sk)
              .addItem(resolve_key(CS_PROMO_SK, catalog_sales.cs_promo_sk))
              .addItem(catalog_sales.cs_order_number)
              .addItem(resolve_integer(CS_PRICING_QUANTITY, catalog_sales.cs_pricing.quantity))
              .addItem(resolve_decimal(CS_PRICING_WHOLESALE_COST, catalog_sales.cs_pricing.wholesale_cost))
              .addItem(resolve_decimal(CS_PRICING_LIST_PRICE, catalog_sales.cs_pricing.list_price))
              .addItem(resolve_decimal(CS_PRICING_SALES_PRICE, catalog_sales.cs_pricing.sales_price))
              .addItem(resolve_decimal(CS_PRICING_EXT_DISCOUNT_AMOUNT, catalog_sales.cs_pricing.ext_discount_amt))
              .addItem(resolve_decimal(CS_PRICING_EXT_SALES_PRICE, catalog_sales.cs_pricing.ext_sales_price))
              .addItem(resolve_decimal(CS_PRICING_EXT_WHOLESALE_COST, catalog_sales.cs_pricing.ext_wholesale_cost))
              .addItem(resolve_decimal(CS_PRICING_EXT_LIST_PRICE, catalog_sales.cs_pricing.ext_list_price))
              .addItem(resolve_decimal(CS_PRICING_EXT_TAX, catalog_sales.cs_pricing.ext_tax))
              .addItem(resolve_decimal(CS_PRICING_COUPON_AMT, catalog_sales.cs_pricing.coupon_amt))
              .addItem(resolve_decimal(CS_PRICING_EXT_SHIP_COST, catalog_sales.cs_pricing.ext_ship_cost))
              .addItem(resolve_decimal(CS_PRICING_NET_PAID, catalog_sales.cs_pricing.net_paid))
              .addItem(resolve_decimal(CS_PRICING_NET_PAID_INC_TAX, catalog_sales.cs_pricing.net_paid_inc_tax))
              .addItem(resolve_decimal(CS_PRICING_NET_PAID_INC_SHIP, catalog_sales.cs_pricing.net_paid_inc_ship))
              .addItem(
                  resolve_decimal(CS_PRICING_NET_PAID_INC_SHIP_TAX, catalog_sales.cs_pricing.net_paid_inc_ship_tax))
              .addItem(resolve_decimal(CS_PRICING_NET_PROFIT, catalog_sales.cs_pricing.net_profit))
              .end();
        }

        if (was_returned != 0) {
          loader_catalog_returns.start()
              .addItem(resolve_key(CR_RETURNED_DATE_SK, catalog_returns.cr_returned_date_sk))
              .addItem(resolve_key(CR_RETURNED_TIME_SK, catalog_returns.cr_returned_time_sk))
              .addItem(catalog_returns.cr_item_sk)
              .addItem(resolve_key(CR_REFUNDED_CUSTOMER_SK, catalog_returns.cr_refunded_customer_sk))
              .addItem(resolve_key(CR_REFUNDED_CDEMO_SK, catalog_returns.cr_refunded_cdemo_sk))
              .addItem(resolve_key(CR_REFUNDED_HDEMO_SK, catalog_returns.cr_refunded_hdemo_sk))
              .addItem(resolve_key(CR_REFUNDED_ADDR_SK, catalog_returns.cr_refunded_addr_sk))
              .addItem(resolve_key(CR_RETURNING_CUSTOMER_SK, catalog_returns.cr_returning_customer_sk))
              .addItem(resolve_key(CR_RETURNING_CDEMO_SK, catalog_returns.cr_returning_cdemo_sk))
              .addItem(resolve_key(CR_RETURNING_HDEMO_SK, catalog_returns.cr_returning_hdemo_sk))
              .addItem(resolve_key(CR_RETURNING_ADDR_SK, catalog_returns.cr_returning_addr_sk))
              .addItem(resolve_key(CR_CALL_CENTER_SK, catalog_returns.cr_call_center_sk))
              .addItem(resolve_key(CR_CATALOG_PAGE_SK, catalog_returns.cr_catalog_page_sk))
              .addItem(resolve_key(CR_SHIP_MODE_SK, catalog_returns.cr_ship_mode_sk))
              .addItem(resolve_key(CR_WAREHOUSE_SK, catalog_returns.cr_warehouse_sk))
              .addItem(resolve_key(CR_REASON_SK, catalog_returns.cr_reason_sk))
              .addItem(catalog_returns.cr_order_number)
              .addItem(resolve_integer(CR_PRICING_QUANTITY, catalog_returns.cr_pricing.quantity))
              .addItem(resolve_decimal(CR_PRICING_NET_PAID, catalog_returns.cr_pricing.net_paid))
              .addItem(resolve_decimal(CR_PRICING_EXT_TAX, catalog_returns.cr_pricing.ext_tax))
              .addItem(resolve_decimal(CR_PRICING_NET_PAID_INC_TAX, catalog_returns.cr_pricing.net_paid_inc_tax))
              .addItem(resolve_decimal(CR_PRICING_FEE, catalog_returns.cr_pricing.fee))
              .addItem(resolve_decimal(CR_PRICING_EXT_SHIP_COST, catalog_returns.cr_pricing.ext_ship_cost))
              .addItem(resolve_decimal(CR_PRICING_REFUNDED_CASH, catalog_returns.cr_pricing.refunded_cash))
              .addItem(resolve_decimal(CR_PRICING_REVERSED_CHARGE, catalog_returns.cr_pricing.reversed_charge))
              .addItem(resolve_decimal(CR_PRICING_STORE_CREDIT, catalog_returns.cr_pricing.store_credit))
              .addItem(resolve_decimal(CR_PRICING_NET_LOSS, catalog_returns.cr_pricing.net_loss))
              .end();
          if (loader_catalog_returns.row_count() == max_row_)
            break;
        }
      }
    }
    tpcds_row_stop(CATALOG_SALES);
  }

  return std::max(loader_catalog_returns.row_count(), loader_catalog_sales.row_count());
}

int TPCDSTableGenerator::generate_customer_address() const {
  auto [customer_address_first, customer_address_count] = prepare_for_table(CUSTOMER_ADDRESS);

  TableLoader loader(table_, 13, 100);
  for (auto customer_address_index = ds_key_t{0}; customer_address_index < customer_address_count;
       ++customer_address_index) {
    const auto customer_address = call_dbgen_mk<W_CUSTOMER_ADDRESS_TBL, &mk_w_customer_address, CUSTOMER_ADDRESS>(
        customer_address_first + customer_address_index);

    loader.start()
        .addItem(resolve_key(CA_ADDRESS_SK, customer_address.ca_addr_sk))
        .addItem(resolve_string(CA_ADDRESS_ID, customer_address.ca_addr_id))
        .addItem(resolve_string(CA_ADDRESS_STREET_NUM, std::to_string(customer_address.ca_address.street_num)))
        .addItem(resolve_street_name(CA_ADDRESS_STREET_NAME1, customer_address.ca_address))
        .addItem(resolve_string(CA_ADDRESS_STREET_TYPE, customer_address.ca_address.street_type))
        .addItem(resolve_string(CA_ADDRESS_SUITE_NUM, customer_address.ca_address.suite_num))
        .addItem(resolve_string(CA_ADDRESS_CITY, customer_address.ca_address.city))
        .addItem(resolve_string(CA_ADDRESS_COUNTY, customer_address.ca_address.county))
        .addItem(resolve_string(CA_ADDRESS_STATE, customer_address.ca_address.state))
        .addItem(resolve_string(CA_ADDRESS_ZIP, zip_to_string(customer_address.ca_address.zip)))
        .addItem(resolve_string(CA_ADDRESS_COUNTRY, customer_address.ca_address.country))
        .addItem(resolve_gmt_offset(CA_ADDRESS_GMT_OFFSET, customer_address.ca_address.gmt_offset))
        .addItem(resolve_string(CA_LOCATION_TYPE, customer_address.ca_location_type))
        .end();

    tpcds_row_stop(CUSTOMER_ADDRESS);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_customer() const {
  auto [customer_first, customer_count] = prepare_for_table(CUSTOMER);

  TableLoader loader(table_, 18, 100);
  for (auto customer_index = ds_key_t{0}; customer_index < customer_count; ++customer_index) {
    const auto customer = call_dbgen_mk<W_CUSTOMER_TBL, &mk_w_customer, CUSTOMER>(customer_first + customer_index);

    loader.start()
        .addItem(resolve_key(C_CUSTOMER_SK, customer.c_customer_sk))
        .addItem(resolve_string(C_CUSTOMER_ID, customer.c_customer_id))
        .addItem(resolve_key(C_CURRENT_CDEMO_SK, customer.c_current_cdemo_sk))
        .addItem(resolve_key(C_CURRENT_HDEMO_SK, customer.c_current_hdemo_sk))
        .addItem(resolve_key(C_CURRENT_ADDR_SK, customer.c_current_addr_sk))
        .addItem(resolve_integer(C_FIRST_SHIPTO_DATE_ID, customer.c_first_shipto_date_id))
        .addItem(resolve_integer(C_FIRST_SALES_DATE_ID, customer.c_first_sales_date_id))
        .addItem(resolve_string(C_SALUTATION, customer.c_salutation))
        .addItem(resolve_string(C_FIRST_NAME, customer.c_first_name))
        .addItem(resolve_string(C_LAST_NAME, customer.c_last_name))
        .addItem(resolve_string(C_PREFERRED_CUST_FLAG, boolean_to_string(customer.c_preferred_cust_flag != 0)))
        .addItem(resolve_integer(C_BIRTH_DAY, customer.c_birth_day))
        .addItem(resolve_integer(C_BIRTH_MONTH, customer.c_birth_month))
        .addItem(resolve_integer(C_BIRTH_YEAR, customer.c_birth_year))
        .addItem(resolve_string(C_BIRTH_COUNTRY, customer.c_birth_country))
        .addItem(resolve_string(C_LOGIN, customer.c_login))
        .addItem(resolve_string(C_EMAIL_ADDRESS, customer.c_email_address))
        .addItem(resolve_integer(C_LAST_REVIEW_DATE, customer.c_last_review_date))
        .end();

    tpcds_row_stop(CUSTOMER);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_customer_demographics() const {
  auto [customer_demographics_first, customer_demographics_count] = prepare_for_table(CUSTOMER_DEMOGRAPHICS);

  TableLoader loader(table_, 9, 100);
  for (auto customer_demographic = ds_key_t{0}; customer_demographic < customer_demographics_count;
       ++customer_demographic) {
    const auto customer_demographics =
        call_dbgen_mk<W_CUSTOMER_DEMOGRAPHICS_TBL, &mk_w_customer_demographics, CUSTOMER_DEMOGRAPHICS>(
            customer_demographics_first + customer_demographic);

    loader.start()
        .addItem(resolve_key(CD_DEMO_SK, customer_demographics.cd_demo_sk))
        .addItem(resolve_string(CD_GENDER, customer_demographics.cd_gender))
        .addItem(resolve_string(CD_MARITAL_STATUS, customer_demographics.cd_marital_status))
        .addItem(resolve_string(CD_EDUCATION_STATUS, customer_demographics.cd_education_status))
        .addItem(resolve_integer(CD_PURCHASE_ESTIMATE, customer_demographics.cd_purchase_estimate))
        .addItem(resolve_string(CD_CREDIT_RATING, customer_demographics.cd_credit_rating))
        .addItem(resolve_integer(CD_DEP_COUNT, customer_demographics.cd_dep_count))
        .addItem(resolve_integer(CD_DEP_EMPLOYED_COUNT, customer_demographics.cd_dep_employed_count))
        .addItem(resolve_integer(CD_DEP_COLLEGE_COUNT, customer_demographics.cd_dep_college_count))
        .end();

    tpcds_row_stop(CUSTOMER_DEMOGRAPHICS);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_date_dim() const {
  auto [date_first, date_count] = prepare_for_table(DATE);
  TableLoader loader(table_, 28, 100);
  for (auto date_index = ds_key_t{0}; date_index < date_count; ++date_index) {
    const auto date = call_dbgen_mk<W_DATE_TBL, &mk_w_date, DATE>(date_first + date_index);

    auto quarter_name = std::to_string(date.d_year) + "Q" + std::to_string(date.d_qoy);

    loader.start()
        .addItem(date.d_date_sk)
        .addItem(resolve_string(D_DATE_ID, date.d_date_id))
        .addItem(resolve_date_id(D_DATE_SK, date.d_date_sk))
        .addItem(resolve_integer(D_MONTH_SEQ, date.d_month_seq))
        .addItem(resolve_integer(D_WEEK_SEQ, date.d_week_seq))
        .addItem(resolve_integer(D_QUARTER_SEQ, date.d_quarter_seq))
        .addItem(resolve_integer(D_YEAR, date.d_year))
        .addItem(resolve_integer(D_DOW, date.d_dow))
        .addItem(resolve_integer(D_MOY, date.d_moy))
        .addItem(resolve_integer(D_DOM, date.d_dom))
        .addItem(resolve_integer(D_QOY, date.d_qoy))
        .addItem(resolve_integer(D_FY_YEAR, date.d_fy_year))
        .addItem(resolve_integer(D_FY_QUARTER_SEQ, date.d_fy_quarter_seq))
        .addItem(resolve_integer(D_FY_WEEK_SEQ, date.d_fy_week_seq))
        .addItem(resolve_string(D_DAY_NAME, date.d_day_name))
        .addItem(resolve_string(D_QUARTER_NAME, std::move(quarter_name)))
        .addItem(resolve_string(D_HOLIDAY, boolean_to_string(date.d_holiday != 0)))
        .addItem(resolve_string(D_WEEKEND, boolean_to_string(date.d_weekend != 0)))
        .addItem(resolve_string(D_FOLLOWING_HOLIDAY, boolean_to_string(date.d_following_holiday != 0)))
        .addItem(resolve_integer(D_FIRST_DOM, date.d_first_dom))
        .addItem(resolve_integer(D_LAST_DOM, date.d_last_dom))
        .addItem(resolve_integer(D_SAME_DAY_LY, date.d_same_day_ly))
        .addItem(resolve_integer(D_SAME_DAY_LQ, date.d_same_day_lq))
        .addItem(resolve_string(D_CURRENT_DAY, boolean_to_string(date.d_current_day != 0)))
        .addItem(resolve_string(D_CURRENT_WEEK, boolean_to_string(date.d_current_week != 0)))
        .addItem(resolve_string(D_CURRENT_MONTH, boolean_to_string(date.d_current_month != 0)))
        .addItem(resolve_string(D_CURRENT_QUARTER, boolean_to_string(date.d_current_quarter != 0)))
        .addItem(resolve_string(D_CURRENT_YEAR, boolean_to_string(date.d_current_year != 0)))
        .end();
    tpcds_row_stop(DATE);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_household_demographics() const {
  auto [household_demographics_first, household_demographics_count] = prepare_for_table(HOUSEHOLD_DEMOGRAPHICS);

  TableLoader loader(table_, 5, 100);

  for (auto household_demographic = ds_key_t{0}; household_demographic < household_demographics_count;
       ++household_demographic) {
    const auto household_demographics =
        call_dbgen_mk<W_HOUSEHOLD_DEMOGRAPHICS_TBL, &mk_w_household_demographics, HOUSEHOLD_DEMOGRAPHICS>(
            household_demographics_first + household_demographic);

    loader.start()
        .addItem(household_demographics.hd_demo_sk)
        .addItem(resolve_key(HD_INCOME_BAND_ID, household_demographics.hd_income_band_id))
        .addItem(resolve_string(HD_BUY_POTENTIAL, household_demographics.hd_buy_potential))
        .addItem(resolve_integer(HD_DEP_COUNT, household_demographics.hd_dep_count))
        .addItem(resolve_integer(HD_VEHICLE_COUNT, household_demographics.hd_vehicle_count))
        .end();

    tpcds_row_stop(HOUSEHOLD_DEMOGRAPHICS);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_income_band() const {
  auto [income_band_first, income_band_count] = prepare_for_table(INCOME_BAND);
  TableLoader loader(table_, 3, 100);

  for (auto income_band_index = ds_key_t{0}; income_band_index < income_band_count; ++income_band_index) {
    const auto income_band =
        call_dbgen_mk<W_INCOME_BAND_TBL, &mk_w_income_band, INCOME_BAND>(income_band_first + income_band_index);

    loader.start()
        .addItem(resolve_key(IB_INCOME_BAND_ID, income_band.ib_income_band_id))
        .addItem(resolve_integer(IB_LOWER_BOUND, income_band.ib_lower_bound))
        .addItem(resolve_integer(IB_UPPER_BOUND, income_band.ib_upper_bound))
        .end();
    tpcds_row_stop(INCOME_BAND);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_inventory() const {
  auto [inventory_first, inventory_count] = prepare_for_table(INVENTORY);

  TableLoader loader(table_, 4, 100);

  for (auto inventory_index = ds_key_t{0}; inventory_index < inventory_count; ++inventory_index) {
    const auto inventory =
        call_dbgen_mk<W_INVENTORY_TBL, &mk_w_inventory, INVENTORY>(inventory_first + inventory_index);

    loader.start()
        .addItem(resolve_key(INV_DATE_SK, inventory.inv_date_sk))
        .addItem(resolve_key(INV_ITEM_SK, inventory.inv_item_sk))
        .addItem(resolve_key(INV_WAREHOUSE_SK, inventory.inv_warehouse_sk))
        .addItem(resolve_integer(INV_QUANTITY_ON_HAND, inventory.inv_quantity_on_hand))
        .end();
    tpcds_row_stop(INVENTORY);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_item() const {
  auto [item_first, item_count] = prepare_for_table(ITEM);

  TableLoader loader(table_, 22, 100);

  for (auto item_index = ds_key_t{0}; item_index < item_count; ++item_index) {
    const auto item = call_dbgen_mk<W_ITEM_TBL, &mk_w_item, ITEM>(item_first + item_index);

    loader.start()
        .addItem(resolve_key(I_ITEM_SK, item.i_item_sk))
        .addItem(resolve_string(I_ITEM_ID, item.i_item_id))
        .addItem(resolve_date_id(I_REC_START_DATE_ID, item.i_rec_start_date_id))
        .addItem(resolve_date_id(I_REC_END_DATE_ID, item.i_rec_end_date_id))
        .addItem(resolve_string(I_ITEM_DESC, item.i_item_desc))
        .addItem(resolve_decimal(I_CURRENT_PRICE, item.i_current_price))
        .addItem(resolve_decimal(I_WHOLESALE_COST, item.i_wholesale_cost))
        .addItem(resolve_key(I_BRAND_ID, item.i_brand_id))
        .addItem(resolve_string(I_BRAND, item.i_brand))
        .addItem(resolve_key(I_CLASS_ID, item.i_class_id))
        .addItem(resolve_string(I_CLASS, item.i_class))
        .addItem(resolve_key(I_CATEGORY_ID, item.i_category_id))
        .addItem(resolve_string(I_CATEGORY, item.i_category))
        .addItem(resolve_key(I_MANUFACT_ID, item.i_manufact_id))
        .addItem(resolve_string(I_MANUFACT, item.i_manufact))
        .addItem(resolve_string(I_SIZE, item.i_size))
        .addItem(resolve_string(I_FORMULATION, item.i_formulation))
        .addItem(resolve_string(I_COLOR, item.i_color))
        .addItem(resolve_string(I_UNITS, item.i_units))
        .addItem(resolve_string(I_CONTAINER, item.i_container))
        .addItem(resolve_key(I_MANAGER_ID, item.i_manager_id))
        .addItem(resolve_string(I_PRODUCT_NAME, item.i_product_name))
        .end();
    tpcds_row_stop(ITEM);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_promotion() const {
  auto [promotion_first, promotion_count] = prepare_for_table(PROMOTION);

  TableLoader loader(table_, 19, 100);

  for (auto promotion_index = ds_key_t{0}; promotion_index < promotion_count; ++promotion_index) {
    const auto promotion =
        call_dbgen_mk<W_PROMOTION_TBL, &mk_w_promotion, PROMOTION>(promotion_first + promotion_index);

    loader.start()
        .addItem(resolve_key(P_PROMO_SK, promotion.p_promo_sk))
        .addItem(resolve_string(P_PROMO_ID, promotion.p_promo_id))
        .addItem(resolve_key(P_START_DATE_ID, promotion.p_start_date_id))
        .addItem(resolve_key(P_END_DATE_ID, promotion.p_end_date_id))
        .addItem(resolve_key(P_ITEM_SK, promotion.p_item_sk))
        .addItem(resolve_decimal(P_COST, promotion.p_cost))
        .addItem(resolve_integer(P_RESPONSE_TARGET, promotion.p_response_target))
        .addItem(resolve_string(P_PROMO_NAME, promotion.p_promo_name))
        .addItem(resolve_string(P_CHANNEL_DMAIL, boolean_to_string(promotion.p_channel_dmail != 0)))
        .addItem(resolve_string(P_CHANNEL_EMAIL, boolean_to_string(promotion.p_channel_email != 0)))
        .addItem(resolve_string(P_CHANNEL_CATALOG, boolean_to_string(promotion.p_channel_catalog != 0)))
        .addItem(resolve_string(P_CHANNEL_TV, boolean_to_string(promotion.p_channel_tv != 0)))
        .addItem(resolve_string(P_CHANNEL_RADIO, boolean_to_string(promotion.p_channel_radio != 0)))
        .addItem(resolve_string(P_CHANNEL_PRESS, boolean_to_string(promotion.p_channel_press != 0)))
        .addItem(resolve_string(P_CHANNEL_EVENT, boolean_to_string(promotion.p_channel_event != 0)))
        .addItem(resolve_string(P_CHANNEL_DEMO, boolean_to_string(promotion.p_channel_demo != 0)))
        .addItem(resolve_string(P_CHANNEL_DETAILS, promotion.p_channel_details))
        .addItem(resolve_string(P_PURPOSE, promotion.p_purpose))
        .addItem(resolve_string(P_DISCOUNT_ACTIVE, boolean_to_string(promotion.p_discount_active != 0)))
        .end();
    tpcds_row_stop(PROMOTION);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_reason() const {
  auto [reason_first, reason_count] = prepare_for_table(REASON);

  TableLoader loader(table_, 3, 100);

  for (auto reason_index = ds_key_t{0}; reason_index < reason_count; ++reason_index) {
    const auto reason = call_dbgen_mk<W_REASON_TBL, &mk_w_reason, REASON>(reason_first + reason_index);

    loader.start()
        .addItem(resolve_key(R_REASON_SK, reason.r_reason_sk))
        .addItem(resolve_string(R_REASON_ID, reason.r_reason_id))
        .addItem(resolve_string(R_REASON_DESCRIPTION, reason.r_reason_description))
        .end();
    tpcds_row_stop(REASON);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_ship_mode() const {
  auto [ship_mode_first, ship_mode_count] = prepare_for_table(SHIP_MODE);

  TableLoader loader(table_, 6, 100);

  for (auto ship_mode_index = ds_key_t{0}; ship_mode_index < ship_mode_count; ++ship_mode_index) {
    const auto ship_mode =
        call_dbgen_mk<W_SHIP_MODE_TBL, &mk_w_ship_mode, SHIP_MODE>(ship_mode_first + ship_mode_index);

    loader.start()
        .addItem(resolve_key(SM_SHIP_MODE_SK, ship_mode.sm_ship_mode_sk))
        .addItem(resolve_string(SM_SHIP_MODE_ID, ship_mode.sm_ship_mode_id))
        .addItem(resolve_string(SM_TYPE, ship_mode.sm_type))
        .addItem(resolve_string(SM_CODE, ship_mode.sm_code))
        .addItem(resolve_string(SM_CARRIER, ship_mode.sm_carrier))
        .addItem(resolve_string(SM_CONTRACT, ship_mode.sm_contract))
        .end();
    tpcds_row_stop(SHIP_MODE);

    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_store() const {
  auto [store_first, store_count] = prepare_for_table(STORE);

  TableLoader loader(table_, 29, 100);

  for (auto store_index = ds_key_t{0}; store_index < store_count; ++store_index) {
    const auto store = call_dbgen_mk<W_STORE_TBL, &mk_w_store, STORE>(store_first + store_index);

    loader.start()
        .addItem(store.store_sk)
        .addItem(resolve_string(W_STORE_ID, store.store_id))
        .addItem(resolve_date_id(W_STORE_REC_START_DATE_ID, store.rec_start_date_id))
        .addItem(resolve_date_id(W_STORE_REC_END_DATE_ID, store.rec_end_date_id))
        .addItem(resolve_key(W_STORE_CLOSED_DATE_ID, store.closed_date_id))
        .addItem(resolve_string(W_STORE_NAME, store.store_name))
        .addItem(resolve_integer(W_STORE_EMPLOYEES, store.employees))
        .addItem(resolve_integer(W_STORE_FLOOR_SPACE, store.floor_space))
        .addItem(resolve_string(W_STORE_HOURS, store.hours))
        .addItem(resolve_string(W_STORE_MANAGER, store.store_manager))
        .addItem(resolve_integer(W_STORE_MARKET_ID, store.market_id))
        .addItem(resolve_string(W_STORE_GEOGRAPHY_CLASS, store.geography_class))
        .addItem(resolve_string(W_STORE_MARKET_DESC, store.market_desc))
        .addItem(resolve_string(W_STORE_MARKET_MANAGER, store.market_manager))
        .addItem(resolve_key(W_STORE_DIVISION_ID, store.division_id))
        .addItem(resolve_string(W_STORE_DIVISION_NAME, store.division_name))
        .addItem(resolve_key(W_STORE_COMPANY_ID, store.company_id))
        .addItem(resolve_string(W_STORE_COMPANY_NAME, store.company_name))
        .addItem(resolve_string(W_STORE_ADDRESS_STREET_NUM, std::to_string(store.address.street_num)))
        .addItem(resolve_street_name(W_STORE_ADDRESS_STREET_NAME1, store.address))
        .addItem(resolve_string(W_STORE_ADDRESS_STREET_TYPE, store.address.street_type))
        .addItem(resolve_string(W_STORE_ADDRESS_SUITE_NUM, store.address.suite_num))
        .addItem(resolve_string(W_STORE_ADDRESS_CITY, store.address.city))
        .addItem(resolve_string(W_STORE_ADDRESS_COUNTY, store.address.county))
        .addItem(resolve_string(W_STORE_ADDRESS_STATE, store.address.state))
        .addItem(resolve_string(W_STORE_ADDRESS_ZIP, zip_to_string(store.address.zip)))
        .addItem(resolve_string(W_STORE_ADDRESS_COUNTRY, store.address.country))
        .addItem(resolve_gmt_offset(W_STORE_ADDRESS_GMT_OFFSET, store.address.gmt_offset))
        .addItem(resolve_decimal(W_STORE_TAX_PERCENTAGE, store.dTaxPercentage))
        .end();
    tpcds_row_stop(STORE);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_store_sales_and_returns() const {
  auto [store_sales_first, store_sales_count] = prepare_for_table(STORE_SALES);

  TableLoader loader_store_sales("store_sales", 23, 100);
  TableLoader loader_store_returns("store_returns", 20, 100);

  for (auto store_sale = ds_key_t{0}; store_sale < store_sales_count; ++store_sale) {
    auto store_sales = W_STORE_SALES_TBL{};
    auto store_returns = W_STORE_RETURNS_TBL{};

    // modified call to mk_w_store_sales(&store_sales, store_sales_first + store_sale, &store_returns, &was_returned)
    {
      mk_w_store_sales_master(&store_sales, store_sales_first + store_sale, 0);

      auto n_lineitems = int{0};
      genrand_integer(&n_lineitems, DIST_UNIFORM, 8, 16, 0, SS_TICKET_NUMBER);
      for (auto lineitem_index = int{0}; lineitem_index < n_lineitems; ++lineitem_index) {
        auto was_returned = int{0};
        mk_w_store_sales_detail(&store_sales, 0, &store_returns, &was_returned);

        if (loader_store_sales.row_count() < max_row_) {
          loader_store_sales.start()
              .addItem(resolve_key(SS_SOLD_DATE_SK, store_sales.ss_sold_date_sk))
              .addItem(resolve_key(SS_SOLD_TIME_SK, store_sales.ss_sold_time_sk))
              .addItem(resolve_key(SS_SOLD_ITEM_SK, store_sales.ss_sold_item_sk))
              .addItem(resolve_key(SS_SOLD_CUSTOMER_SK, store_sales.ss_sold_customer_sk))
              .addItem(resolve_key(SS_SOLD_CDEMO_SK, store_sales.ss_sold_cdemo_sk))
              .addItem(resolve_key(SS_SOLD_HDEMO_SK, store_sales.ss_sold_hdemo_sk))
              .addItem(resolve_key(SS_SOLD_ADDR_SK, store_sales.ss_sold_addr_sk))
              .addItem(resolve_key(SS_SOLD_STORE_SK, store_sales.ss_sold_store_sk))
              .addItem(resolve_key(SS_SOLD_PROMO_SK, store_sales.ss_sold_promo_sk))
              .addItem(resolve_integer(SS_TICKET_NUMBER, store_sales.ss_ticket_number))
              .addItem(resolve_integer(SS_PRICING_QUANTITY, store_sales.ss_pricing.quantity))
              .addItem(resolve_decimal(SS_PRICING_WHOLESALE_COST, store_sales.ss_pricing.wholesale_cost))
              .addItem(resolve_decimal(SS_PRICING_LIST_PRICE, store_sales.ss_pricing.list_price))
              .addItem(resolve_decimal(SS_PRICING_SALES_PRICE, store_sales.ss_pricing.sales_price))
              .addItem(resolve_decimal(SS_PRICING_COUPON_AMT, store_sales.ss_pricing.coupon_amt))
              .addItem(resolve_decimal(SS_PRICING_EXT_SALES_PRICE, store_sales.ss_pricing.ext_sales_price))
              .addItem(resolve_decimal(SS_PRICING_EXT_WHOLESALE_COST, store_sales.ss_pricing.ext_wholesale_cost))
              .addItem(resolve_decimal(SS_PRICING_EXT_LIST_PRICE, store_sales.ss_pricing.ext_list_price))
              .addItem(resolve_decimal(SS_PRICING_EXT_TAX, store_sales.ss_pricing.ext_tax))
              .addItem(resolve_decimal(SS_PRICING_COUPON_AMT, store_sales.ss_pricing.coupon_amt))
              .addItem(resolve_decimal(SS_PRICING_NET_PAID, store_sales.ss_pricing.net_paid))
              .addItem(resolve_decimal(SS_PRICING_NET_PAID_INC_TAX, store_sales.ss_pricing.net_paid_inc_tax))
              .addItem(resolve_decimal(SS_PRICING_NET_PROFIT, store_sales.ss_pricing.net_profit))
              .end();
          // dsdgen prints coupon_amt twice, so we do too...
        }

        if (was_returned != 0) {
          loader_store_returns.start()
              .addItem(resolve_key(SR_RETURNED_DATE_SK, store_returns.sr_returned_date_sk))
              .addItem(resolve_key(SR_RETURNED_TIME_SK, store_returns.sr_returned_time_sk))
              .addItem(resolve_key(SR_ITEM_SK, store_returns.sr_item_sk))
              .addItem(resolve_key(SR_CUSTOMER_SK, store_returns.sr_customer_sk))
              .addItem(resolve_key(SR_CDEMO_SK, store_returns.sr_cdemo_sk))
              .addItem(resolve_key(SR_HDEMO_SK, store_returns.sr_hdemo_sk))
              .addItem(resolve_key(SR_ADDR_SK, store_returns.sr_addr_sk))
              .addItem(resolve_key(SR_STORE_SK, store_returns.sr_store_sk))
              .addItem(resolve_key(SR_REASON_SK, store_returns.sr_reason_sk))
              .addItem(resolve_integer(SR_TICKET_NUMBER, store_returns.sr_ticket_number))
              .addItem(resolve_integer(SR_PRICING_QUANTITY, store_returns.sr_pricing.quantity))
              .addItem(resolve_decimal(SR_PRICING_NET_PAID, store_returns.sr_pricing.net_paid))
              .addItem(resolve_decimal(SR_PRICING_EXT_TAX, store_returns.sr_pricing.ext_tax))
              .addItem(resolve_decimal(SR_PRICING_NET_PAID_INC_TAX, store_returns.sr_pricing.net_paid_inc_tax))
              .addItem(resolve_decimal(SR_PRICING_FEE, store_returns.sr_pricing.fee))
              .addItem(resolve_decimal(SR_PRICING_EXT_SHIP_COST, store_returns.sr_pricing.ext_ship_cost))
              .addItem(resolve_decimal(SR_PRICING_REFUNDED_CASH, store_returns.sr_pricing.refunded_cash))
              .addItem(resolve_decimal(SR_PRICING_REVERSED_CHARGE, store_returns.sr_pricing.reversed_charge))
              .addItem(resolve_decimal(SR_PRICING_STORE_CREDIT, store_returns.sr_pricing.store_credit))
              .addItem(resolve_decimal(SR_PRICING_NET_LOSS, store_returns.sr_pricing.net_loss))
              .end();
          if (loader_store_returns.row_count() >= max_row_)
            break;
        }
      }
    }
    tpcds_row_stop(STORE_SALES);
  }

  return std::max(loader_store_sales.row_count(), loader_store_returns.row_count());
}

int TPCDSTableGenerator::generate_time_dim() const {
  auto [time_first, time_count] = prepare_for_table(TIME);

  TableLoader loader(table_, 10, 100);

  for (auto time_index = ds_key_t{0}; time_index < time_count; ++time_index) {
    const auto time = call_dbgen_mk<W_TIME_TBL, &mk_w_time, TIME>(time_first + time_index);

    loader.start()
        .addItem(resolve_key(T_TIME_SK, time.t_time_sk))
        .addItem(resolve_string(T_TIME_ID, time.t_time_id))
        .addItem(resolve_integer(T_TIME, time.t_time))
        .addItem(resolve_integer(T_HOUR, time.t_hour))
        .addItem(resolve_integer(T_MINUTE, time.t_minute))
        .addItem(resolve_integer(T_SECOND, time.t_second))
        .addItem(resolve_string(T_AM_PM, time.t_am_pm))
        .addItem(resolve_string(T_SHIFT, time.t_shift))
        .addItem(resolve_string(T_SUB_SHIFT, time.t_sub_shift))
        .addItem(resolve_string(T_MEAL_TIME, time.t_meal_time))
        .end();
    tpcds_row_stop(TIME);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_warehouse() const {
  auto [warehouse_first, warehouse_count] = prepare_for_table(WAREHOUSE);

  TableLoader loader(table_, 14, 100);

  for (auto warehouse_index = ds_key_t{0}; warehouse_index < warehouse_count; ++warehouse_index) {
    const auto warehouse =
        call_dbgen_mk<W_WAREHOUSE_TBL, &mk_w_warehouse, WAREHOUSE>(warehouse_first + warehouse_index);

    loader.start()
        .addItem(resolve_key(W_WAREHOUSE_SK, warehouse.w_warehouse_sk))
        .addItem(resolve_string(W_WAREHOUSE_ID, warehouse.w_warehouse_id))
        .addItem(resolve_string(W_WAREHOUSE_NAME, warehouse.w_warehouse_name))
        .addItem(resolve_integer(W_WAREHOUSE_SQ_FT, warehouse.w_warehouse_sq_ft))
        .addItem(resolve_string(W_ADDRESS_STREET_NUM, std::to_string(warehouse.w_address.street_num)))
        .addItem(resolve_street_name(W_ADDRESS_STREET_NAME1, warehouse.w_address))
        .addItem(resolve_string(W_ADDRESS_STREET_TYPE, warehouse.w_address.street_type))
        .addItem(resolve_string(W_ADDRESS_SUITE_NUM, warehouse.w_address.suite_num))
        .addItem(resolve_string(W_ADDRESS_CITY, warehouse.w_address.city))
        .addItem(resolve_string(W_ADDRESS_COUNTY, warehouse.w_address.county))
        .addItem(resolve_string(W_ADDRESS_STATE, warehouse.w_address.state))
        .addItem(resolve_string(W_ADDRESS_ZIP, zip_to_string(warehouse.w_address.zip)))
        .addItem(resolve_string(W_ADDRESS_COUNTRY, warehouse.w_address.country))
        .addItem(resolve_gmt_offset(W_ADDRESS_GMT_OFFSET, warehouse.w_address.gmt_offset))
        .end();
    tpcds_row_stop(WAREHOUSE);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_web_page() const {
  auto [web_page_first, web_page_count] = prepare_for_table(WEB_PAGE);

  TableLoader loader(table_, 14, 100);

  for (auto web_page_index = ds_key_t{0}; web_page_index < web_page_count; ++web_page_index) {
    const auto web_page = call_dbgen_mk<W_WEB_PAGE_TBL, &mk_w_web_page, WEB_PAGE>(web_page_first + web_page_index);

    loader.start()
        .addItem(resolve_key(WP_PAGE_SK, web_page.wp_page_sk))
        .addItem(resolve_string(WP_PAGE_ID, web_page.wp_page_id))
        .addItem(resolve_date_id(WP_REC_START_DATE_ID, web_page.wp_rec_start_date_id))
        .addItem(resolve_date_id(WP_REC_END_DATE_ID, web_page.wp_rec_end_date_id))
        .addItem(resolve_key(WP_CREATION_DATE_SK, web_page.wp_creation_date_sk))
        .addItem(resolve_key(WP_ACCESS_DATE_SK, web_page.wp_access_date_sk))
        .addItem(resolve_string(WP_AUTOGEN_FLAG, boolean_to_string(web_page.wp_autogen_flag != 0)))
        .addItem(resolve_key(WP_CUSTOMER_SK, web_page.wp_customer_sk))
        .addItem(resolve_string(WP_URL, web_page.wp_url))
        .addItem(resolve_string(WP_TYPE, web_page.wp_type))
        .addItem(resolve_integer(WP_CHAR_COUNT, web_page.wp_char_count))
        .addItem(resolve_integer(WP_LINK_COUNT, web_page.wp_link_count))
        .addItem(resolve_integer(WP_IMAGE_COUNT, web_page.wp_image_count))
        .addItem(resolve_integer(WP_MAX_AD_COUNT, web_page.wp_max_ad_count))
        .end();
    tpcds_row_stop(WEB_PAGE);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

int TPCDSTableGenerator::generate_web_sales_and_returns() const {
  auto [web_sales_first, web_sales_count] = prepare_for_table(WEB_SALES);

  TableLoader loader_web_sales("web_sales", 34, 100);
  TableLoader loader_web_returns("web_returns", 24, 100);

  for (auto web_sales_index = ds_key_t{0}; web_sales_index < web_sales_count; ++web_sales_index) {
    auto web_sales = W_WEB_SALES_TBL{};
    auto web_returns = W_WEB_RETURNS_TBL{};

    // modified call to mk_w_web_sales(&web_sales, web_sales_first + web_sales_index, &web_returns, &was_returned);
    {
      mk_w_web_sales_master(&web_sales, web_sales_first + web_sales_index, 0);

      auto n_lineitems = int{0};
      genrand_integer(&n_lineitems, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER);
      for (auto lineitem_index = int{0}; lineitem_index < n_lineitems; ++lineitem_index) {
        auto was_returned = 0;
        mk_w_web_sales_detail(&web_sales, 0, &web_returns, &was_returned, 0);

        if (loader_web_sales.row_count() < max_row_) {
          loader_web_sales.start()
              .addItem(resolve_key(WS_SOLD_DATE_SK, web_sales.ws_sold_date_sk))
              .addItem(resolve_key(WS_SOLD_TIME_SK, web_sales.ws_sold_time_sk))
              .addItem(resolve_key(WS_SHIP_DATE_SK, web_sales.ws_ship_date_sk))
              .addItem(resolve_key(WS_ITEM_SK, web_sales.ws_item_sk))
              .addItem(resolve_key(WS_BILL_CUSTOMER_SK, web_sales.ws_bill_customer_sk))
              .addItem(resolve_key(WS_BILL_CDEMO_SK, web_sales.ws_bill_cdemo_sk))
              .addItem(resolve_key(WS_BILL_HDEMO_SK, web_sales.ws_bill_hdemo_sk))
              .addItem(resolve_key(WS_BILL_ADDR_SK, web_sales.ws_bill_addr_sk))
              .addItem(resolve_key(WS_SHIP_CUSTOMER_SK, web_sales.ws_ship_customer_sk))
              .addItem(resolve_key(WS_SHIP_CDEMO_SK, web_sales.ws_ship_cdemo_sk))
              .addItem(resolve_key(WS_SHIP_HDEMO_SK, web_sales.ws_ship_hdemo_sk))
              .addItem(resolve_key(WS_SHIP_ADDR_SK, web_sales.ws_ship_addr_sk))
              .addItem(resolve_key(WS_WEB_PAGE_SK, web_sales.ws_web_page_sk))
              .addItem(resolve_key(WS_WEB_SITE_SK, web_sales.ws_web_site_sk))
              .addItem(resolve_key(WS_SHIP_MODE_SK, web_sales.ws_ship_mode_sk))
              .addItem(resolve_key(WS_WAREHOUSE_SK, web_sales.ws_warehouse_sk))
              .addItem(resolve_key(WS_PROMO_SK, web_sales.ws_promo_sk))
              .addItem(resolve_integer(WS_ORDER_NUMBER, web_sales.ws_order_number))
              .addItem(resolve_integer(WS_PRICING_QUANTITY, web_sales.ws_pricing.quantity))
              .addItem(resolve_decimal(WS_PRICING_WHOLESALE_COST, web_sales.ws_pricing.wholesale_cost))
              .addItem(resolve_decimal(WS_PRICING_LIST_PRICE, web_sales.ws_pricing.list_price))
              .addItem(resolve_decimal(WS_PRICING_SALES_PRICE, web_sales.ws_pricing.sales_price))
              .addItem(resolve_decimal(WS_PRICING_EXT_DISCOUNT_AMT, web_sales.ws_pricing.ext_discount_amt))
              .addItem(resolve_decimal(WS_PRICING_EXT_SALES_PRICE, web_sales.ws_pricing.ext_sales_price))
              .addItem(resolve_decimal(WS_PRICING_EXT_WHOLESALE_COST, web_sales.ws_pricing.ext_wholesale_cost))
              .addItem(resolve_decimal(WS_PRICING_EXT_LIST_PRICE, web_sales.ws_pricing.ext_list_price))
              .addItem(resolve_decimal(WS_PRICING_EXT_TAX, web_sales.ws_pricing.ext_tax))
              .addItem(resolve_decimal(WS_PRICING_COUPON_AMT, web_sales.ws_pricing.coupon_amt))
              .addItem(resolve_decimal(WS_PRICING_EXT_SHIP_COST, web_sales.ws_pricing.ext_ship_cost))
              .addItem(resolve_decimal(WS_PRICING_NET_PAID, web_sales.ws_pricing.net_paid))
              .addItem(resolve_decimal(WS_PRICING_NET_PAID_INC_TAX, web_sales.ws_pricing.net_paid_inc_tax))
              .addItem(resolve_decimal(WS_PRICING_NET_PAID_INC_SHIP, web_sales.ws_pricing.net_paid_inc_ship))
              .addItem(resolve_decimal(WS_PRICING_NET_PAID_INC_SHIP_TAX, web_sales.ws_pricing.net_paid_inc_ship_tax))
              .addItem(resolve_decimal(WS_PRICING_NET_PROFIT, web_sales.ws_pricing.net_profit))
              .end();
        }

        if (was_returned != 0) {
          loader_web_returns.start()
              .addItem(resolve_key(WR_RETURNED_DATE_SK, web_returns.wr_returned_date_sk))
              .addItem(resolve_key(WR_RETURNED_TIME_SK, web_returns.wr_returned_time_sk))
              .addItem(resolve_key(WR_ITEM_SK, web_returns.wr_item_sk))
              .addItem(resolve_key(WR_REFUNDED_CUSTOMER_SK, web_returns.wr_refunded_customer_sk))
              .addItem(resolve_key(WR_REFUNDED_CDEMO_SK, web_returns.wr_refunded_cdemo_sk))
              .addItem(resolve_key(WR_REFUNDED_HDEMO_SK, web_returns.wr_refunded_hdemo_sk))
              .addItem(resolve_key(WR_REFUNDED_ADDR_SK, web_returns.wr_refunded_addr_sk))
              .addItem(resolve_key(WR_RETURNING_CUSTOMER_SK, web_returns.wr_returning_customer_sk))
              .addItem(resolve_key(WR_RETURNING_CDEMO_SK, web_returns.wr_returning_cdemo_sk))
              .addItem(resolve_key(WR_RETURNING_HDEMO_SK, web_returns.wr_returning_hdemo_sk))
              .addItem(resolve_key(WR_RETURNING_ADDR_SK, web_returns.wr_returning_addr_sk))
              .addItem(resolve_key(WR_WEB_PAGE_SK, web_returns.wr_web_page_sk))
              .addItem(resolve_key(WR_REASON_SK, web_returns.wr_reason_sk))
              .addItem(resolve_integer(WR_ORDER_NUMBER, web_returns.wr_order_number))
              .addItem(resolve_integer(WR_PRICING_QUANTITY, web_returns.wr_pricing.quantity))
              .addItem(resolve_decimal(WR_PRICING_NET_PAID, web_returns.wr_pricing.net_paid))
              .addItem(resolve_decimal(WR_PRICING_EXT_TAX, web_returns.wr_pricing.ext_tax))
              .addItem(resolve_decimal(WR_PRICING_NET_PAID_INC_TAX, web_returns.wr_pricing.net_paid_inc_tax))
              .addItem(resolve_decimal(WR_PRICING_FEE, web_returns.wr_pricing.fee))
              .addItem(resolve_decimal(WR_PRICING_EXT_SHIP_COST, web_returns.wr_pricing.ext_ship_cost))
              .addItem(resolve_decimal(WR_PRICING_REFUNDED_CASH, web_returns.wr_pricing.refunded_cash))
              .addItem(resolve_decimal(WR_PRICING_REVERSED_CHARGE, web_returns.wr_pricing.reversed_charge))
              .addItem(resolve_decimal(WR_PRICING_STORE_CREDIT, web_returns.wr_pricing.store_credit))
              .addItem(resolve_decimal(WR_PRICING_NET_LOSS, web_returns.wr_pricing.net_loss))
              .end();
          if (loader_web_returns.row_count() >= max_row_)
            break;
        }
      }
    }
    tpcds_row_stop(WEB_SALES);
  }

  return std::max(loader_web_sales.row_count(), loader_web_returns.row_count());
}

int TPCDSTableGenerator::generate_web_site() const {
  auto [web_site_first, web_site_count] = prepare_for_table(WEB_SITE);

  TableLoader loader(table_, 26, 100);
  auto web_site = W_WEB_SITE_TBL{};
  static_assert(sizeof(web_site.web_class) == 51);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  const auto snprintf_rc = std::snprintf(web_site.web_class, sizeof(web_site.web_class), "%s", "Unknown");
  // Assert(snprintf_rc > 0, "Unexpected string to parse.");
  for (auto web_site_index = ds_key_t{0}; web_site_index < web_site_count; ++web_site_index) {
    // mk_w_web_site needs a pointer to the previous result because it expects values set previously to still be there
    mk_w_web_site(&web_site, web_site_first + web_site_index);

    loader.start()
        .addItem(resolve_key(WEB_SITE_SK, web_site.web_site_sk))
        .addItem(resolve_string(WEB_SITE_ID, web_site.web_site_id))
        .addItem(resolve_date_id(WEB_REC_START_DATE_ID, web_site.web_rec_start_date_id))
        .addItem(resolve_date_id(WEB_REC_END_DATE_ID, web_site.web_rec_end_date_id))
        .addItem(resolve_string(WEB_NAME, web_site.web_name))
        .addItem(resolve_key(WEB_OPEN_DATE, web_site.web_open_date))
        .addItem(resolve_key(WEB_CLOSE_DATE, web_site.web_close_date))
        .addItem(resolve_string(WEB_CLASS, web_site.web_class))
        .addItem(resolve_string(WEB_MANAGER, web_site.web_manager))
        .addItem(resolve_integer(WEB_MARKET_ID, web_site.web_market_id))
        .addItem(resolve_string(WEB_MARKET_CLASS, web_site.web_market_class))
        .addItem(resolve_string(WEB_MARKET_DESC, web_site.web_market_desc))
        .addItem(resolve_string(WEB_MARKET_MANAGER, web_site.web_market_manager))
        .addItem(resolve_integer(WEB_COMPANY_ID, web_site.web_company_id))
        .addItem(resolve_string(WEB_COMPANY_NAME, web_site.web_company_name))
        .addItem(resolve_string(WEB_ADDRESS_STREET_NUM, std::to_string(web_site.web_address.street_num)))
        .addItem(resolve_street_name(WEB_ADDRESS_STREET_NAME1, web_site.web_address))
        .addItem(resolve_string(WEB_ADDRESS_STREET_TYPE, web_site.web_address.street_type))
        .addItem(resolve_string(WEB_ADDRESS_SUITE_NUM, web_site.web_address.suite_num))
        .addItem(resolve_string(WEB_ADDRESS_CITY, web_site.web_address.city))
        .addItem(resolve_string(WEB_ADDRESS_COUNTY, web_site.web_address.county))
        .addItem(resolve_string(WEB_ADDRESS_STATE, web_site.web_address.state))
        .addItem(resolve_string(WEB_ADDRESS_ZIP, zip_to_string(web_site.web_address.zip)))
        .addItem(resolve_string(WEB_ADDRESS_COUNTRY, web_site.web_address.country))
        .addItem(resolve_gmt_offset(WEB_ADDRESS_GMT_OFFSET, web_site.web_address.gmt_offset))
        .addItem(resolve_decimal(WEB_TAX_PERCENTAGE, web_site.web_tax_percentage))
        .end();
    tpcds_row_stop(WEB_SITE);
    if (loader.row_count() >= max_row_)
      break;
  }

  return loader.row_count();
}

}  // namespace tpcds
