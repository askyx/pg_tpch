#pragma once

#include "dss.h"

namespace tpch {

class TPCHTableGenerator {
 public:
  TPCHTableGenerator(double scale_factor);

  ~TPCHTableGenerator();

  int load_table(char* table, int children, int step);

 private:
  DBGenContext ctx_;
};

}  // namespace tpch
