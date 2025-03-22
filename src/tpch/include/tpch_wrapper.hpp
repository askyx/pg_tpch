
#pragma once

#include <cstdint>

namespace tpch {

struct tpch_runner_result {
  bool is_new;
  int qid;
  double duration;
  double checked;
};

struct TPCHWrapper {
  static void DBGen(double scale);

  static uint32_t QueriesCount();
  static const char* GetQuery(int query);

  static void CreateTPCHSchema();

  static tpch_runner_result* RunTPCH(int qid);
};

}  // namespace tpch
