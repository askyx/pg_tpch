#pragma once

#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tpch {
class TPCHTableGenerator {
 public:
  TPCHTableGenerator(uint32_t scale_factor, const std::string& table, int max_row, std::filesystem::path resource_dir,
                     int rng_seed = 19620718);

  int generate_call_center() const;
  int generate_catalog_page() const;
  int generate_catalog_sales_and_returns() const;
  int generate_customer_address() const;
  int generate_customer() const;
  int generate_customer_demographics() const;
  int generate_date_dim() const;
  int generate_household_demographics() const;
  int generate_income_band() const;
  int generate_inventory() const;
  int generate_item() const;
  int generate_promotion() const;
  int generate_reason() const;
  int generate_ship_mode() const;
  int generate_store() const;
  int generate_store_sales_and_returns() const;
  int generate_time_dim() const;
  int generate_warehouse() const;
  int generate_web_page() const;
  int generate_web_sales_and_returns() const;
  int generate_web_site() const;

 private:
  uint32_t _scale_factor;
  std::string table_;
  int max_row_;
};

}  // namespace tpch
