#pragma once

#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dss.h"

extern "C" {
#include "postgres_ext.h"
}

namespace tpch {

enum class TPCHTable { Part, PartSupp, Supplier, Customer, Orders, LineItem, Nation, Region };
class TPCHTableGenerator {
 public:
  TPCHTableGenerator(double scale_factor, const std::string& table, std::filesystem::path resource_dir,
                     int rng_seed = 19620718);

  ~TPCHTableGenerator();

  int generate_customer();
  std::pair<int, int> generate_orders_and_lineitem();
  int generate_nation();
  std::pair<int, int> generate_part_and_partsupp();
  int generate_region();
  int generate_supplier();

 private:
  std::string table_;
  DBGenContext ctx_;
};

}  // namespace tpch
