#pragma once

#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dss.h"

namespace tpch {

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };
class TPCHTableGenerator {
 public:
  TPCHTableGenerator(uint32_t scale_factor, const std::string& table, int max_row, std::filesystem::path resource_dir,
                     int rng_seed = 19620718);

  ~TPCHTableGenerator();

  int generate_customer();
  int generate_orders_and_lineitem();
  int generate_nation();
  int generate_part_and_partsupp();
  int generate_region();
  int generate_supplier();

 private:
  uint32_t _scale_factor;
  std::string table_;
  int max_row_;
  DBGenContext ctx_;
};

}  // namespace tpch
