
#pragma once

#include <cstdint>
#include <string>

namespace tpch {

struct tpch_runner_result {
  bool is_new;
  int qid;
  double duration;
  double checked;
};

struct TPCHWrapper {
  static std::pair<int, int> DBGen(double scale, char* table);

  static uint32_t QueriesCount();
  static const char* GetQuery(int query);

  static void CreateTPCHSchema();

  static tpch_runner_result* RunTPCH(int qid);
};

}  // namespace tpch
