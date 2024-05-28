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

class TPCHTableGenerator {
 public:
  TPCHTableGenerator(double scale_factor, const std::string& table, int table_id, int children, int step,
                     std::filesystem::path resource_dir, int rng_seed = 19620718);

  ~TPCHTableGenerator();

  std::pair<int, int> generate_customer();
  std::pair<int, int> generate_orders_and_lineitem();
  std::pair<int, int> generate_nation();
  std::pair<int, int> generate_part_and_partsupp();
  std::pair<int, int> generate_region();
  std::pair<int, int> generate_supplier();

 private:
  std::string table_;
  int table_id_;
  DBGenContext ctx_;

  size_t part_offset_{0};
  size_t rowcnt_{0};
};

}  // namespace tpch
