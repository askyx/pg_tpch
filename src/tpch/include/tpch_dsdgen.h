#pragma once

#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tpch {

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };
class TPCHTableGenerator {
 public:
  TPCHTableGenerator(uint32_t scale_factor, const std::string& table, int max_row, std::filesystem::path resource_dir,
                     int rng_seed = 19620718);

  int generate_customer() const;
  int generate_orders_and_lineitem() const;
  int generate_nation() const;
  int generate_part_and_partsupp() const;
  int generate_region() const;
  int generate_supplier() const;

 private:
  uint32_t _scale_factor;
  std::string table_;
  int max_row_;
};

}  // namespace tpch
