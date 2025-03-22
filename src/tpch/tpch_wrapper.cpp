#include "dss.h"
#define ENABLE_NLS

extern "C" {
#include <postgres.h>

#include <access/parallel.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <utils/builtins.h>
#include <utils/palloc.h>

#include "port.h"
}
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <format>
#include <fstream>
#include <stdexcept>
#include <vector>

#include "tpch_constants.hpp"
#include "tpch_dsdgen.h"
#include "tpch_wrapper.hpp"

extern "C" {
PGDLLEXPORT void load_data_impl(dsm_segment *seg, shm_toc *toc);
}
namespace tpch {

static auto get_extension_external_directory(void) {
  char sharepath[MAXPGPATH];

  get_share_path(my_exec_path, sharepath);
  auto path = std::format("{}/extension/tpch", sharepath);
  return std::move(path);
}

class Executor {
 public:
  Executor(const Executor &other) = delete;
  Executor &operator=(const Executor &other) = delete;

  Executor() {
    if (SPI_connect() != SPI_OK_CONNECT)
      throw std::runtime_error("SPI_connect Failed");
  }

  ~Executor() { SPI_finish(); }

  void execute(const std::string &query) const {
    if (auto ret = SPI_exec(query.c_str(), 0); ret < 0)
      throw std::runtime_error(std::format("SPI_exec Failed, get {}", ret));
  }
};

[[maybe_unused]] static double exec_spec(const auto &path, const Executor &executor) {
  if (std::filesystem::exists(path)) {
    std::ifstream file(path);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    const auto start = std::chrono::high_resolution_clock::now();
    executor.execute(sql);
    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
  }
  return 0;
}

void TPCHWrapper::CreateTPCHSchema() {
  const std::filesystem::path extension_dir = get_extension_external_directory();

  Executor executor;

  exec_spec(extension_dir / "pre_prepare.sql", executor);

  auto schema = extension_dir / "schema";
  if (std::filesystem::exists(schema)) {
    std::ranges::for_each(std::filesystem::directory_iterator(schema), [&](const auto &entry) {
      std::ifstream file(entry.path());
      std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
      executor.execute(sql);
    });
  } else
    throw std::runtime_error("Schema file does not exist");

  exec_spec(extension_dir / "post_prepare.sql", executor);
}

uint32_t TPCHWrapper::QueriesCount() {
  return TPCH_QUERIES_COUNT;
}

const char *TPCHWrapper::GetQuery(int query) {
  if (query <= 0 || query > TPCH_QUERIES_COUNT) {
    throw std::runtime_error(std::format("Out of range TPC-DS query number {}", query));
  }

  const std::filesystem::path extension_dir = get_extension_external_directory();

  auto queries = extension_dir / "queries" / std::format("{:02d}.sql", query);
  if (std::filesystem::exists(queries)) {
    std::ifstream file(queries);
    std::string sql((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    return strdup(sql.c_str());
  }
  throw std::runtime_error("Queries file does not exist");
}

tpch_runner_result *TPCHWrapper::RunTPCH(int qid) {
  if (qid < 0 || qid > TPCH_QUERIES_COUNT) {
    throw std::runtime_error(std::format("Out of range TPC-DS query number {}", qid));
  }

  const std::filesystem::path extension_dir = get_extension_external_directory();

  Executor executor;

  auto queries = extension_dir / "queries" / std::format("{:02d}.sql", qid);

  if (std::filesystem::exists(queries)) {
    auto *result = (tpch_runner_result *)palloc(sizeof(tpch_runner_result));

    result->qid = qid;
    result->duration = exec_spec(queries, executor);

    // TODO: check result
    result->checked = true;

    return result;
  } else
    throw std::runtime_error(std::format("Queries file for qid: {} does not exist", qid));
}

struct TableInfo {
  Oid dboid{MyDatabaseId};
  Oid roleoid{GetCurrentRoleId()};
  int table_id;
  int worker_count;
  int current_worker{0};

  // 序列化函数
  void serialize(char *buffer) {
    char *ptr = buffer;

    *(reinterpret_cast<Oid *>(ptr)) = dboid;
    ptr += sizeof(Oid);
    *(reinterpret_cast<Oid *>(ptr)) = roleoid;
    ptr += sizeof(Oid);

    *(reinterpret_cast<int *>(ptr)) = table_id;
    ptr += sizeof(int);

    *(reinterpret_cast<int *>(ptr)) = worker_count;
    ptr += sizeof(int);

    *(reinterpret_cast<int *>(ptr)) = current_worker;
    ptr += sizeof(int);
  }

  void deserialize(const char *buffer) {
    const char *ptr = buffer;

    dboid = *reinterpret_cast<const Oid *>(ptr);
    ptr += sizeof(Oid);
    roleoid = *reinterpret_cast<const Oid *>(ptr);
    ptr += sizeof(Oid);
    table_id = *reinterpret_cast<const int *>(ptr);
    ptr += sizeof(int);
    worker_count = *reinterpret_cast<const int *>(ptr);
    ptr += sizeof(int);
    current_worker = *reinterpret_cast<const int *>(ptr);
    ptr += sizeof(int);
  }
};

void TPCHWrapper::DBGen(double scale) {
  const std::filesystem::path extension_dir = get_extension_external_directory();

  // 8 is 1 part + 7 orders
  auto worker_factor = std::min(1, (int)(max_worker_processes / 8));

  std::vector<TableInfo> table_info{{.table_id = REGION, .worker_count = 1},
                                    {.table_id = NATION, .worker_count = 1},
                                    {.table_id = SUPP, .worker_count = 1},
                                    {.table_id = CUST, .worker_count = 1},
                                    {.table_id = PART_PSUPP, .worker_count = 1 * worker_factor},
                                    {.table_id = ORDER_LINE, .worker_count = 7 * worker_factor}};

  auto worker_count = 4 + (1 + 7) * worker_factor;

  EnterParallelMode();
  auto *pcxt = CreateParallelContext("pg_tpch", "load_data_impl", 1);

  auto *snapshot = RegisterSnapshot(GetTransactionSnapshot());

  InitializeParallelDSM(pcxt);

  if (pcxt->seg == NULL) {
    if (IsMVCCSnapshot(snapshot))
      UnregisterSnapshot(snapshot);
    DestroyParallelContext(pcxt);
    ExitParallelMode();
    throw std::runtime_error("Failed to create parallel context");
  }

  LaunchParallelWorkers(pcxt);

  WaitForParallelWorkersToAttach(pcxt);

  // BackgroundWorker worker;

  // memset(&worker, 0, sizeof(worker));
  // worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  // worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
  // worker.bgw_restart_time = BGW_NEVER_RESTART;
  // sprintf(worker.bgw_library_name, "pg_tpch");
  // sprintf(worker.bgw_function_name, "load_data_impl");
  // snprintf(worker.bgw_name, BGW_MAXLEN, "pg_tpch dynamic worker");
  // snprintf(worker.bgw_type, BGW_MAXLEN, "pg_tpch dynamic");
  // /* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
  // worker.bgw_notify_pid = MyProcPid;
  // worker.bgw_main_arg = Float8GetDatum(scale);

  // std::vector<BackgroundWorkerHandle *> handles(1, nullptr);
  // int i = 0;
  // for (auto &t : table_info) {
  //   for (auto j = 0; j < t.worker_count; ++j) {
  //     t.current_worker = j;
  //     t.serialize(worker.bgw_extra);
  //     if (!RegisterDynamicBackgroundWorker(&worker, &handles[i])) {
  //       --j;
  //       pg_usleep(1000 * 500);  // wait for 500ms before retrying
  //     } else
  //       i++;
  //   }
  // }

  // for (auto *bgwhandle : handles)
  //   WaitForBackgroundWorkerShutdown(bgwhandle);

}  // namespace tpch

}  // namespace tpch

extern "C" {
void load_data_impl(dsm_segment *seg, shm_toc *toc) {
  // tpch::TableInfo table_info;
  // table_info.deserialize(MyBgworkerEntry->bgw_extra);

  // elog(LOG, "Start loading data for table %d, worker %d", table_info.table_id, table_info.current_worker);

  tpch::TPCHTableGenerator generator(1, ORDER_LINE, 7, 0);
  // if (table_info.table_id == PART_PSUPP)
  //   generator.generate_part_and_partsupp();
  // else if (table_info.table_id == ORDER_LINE)
    generator.generate_orders_and_lineitem();
  // else if (table_info.table_id == CUST)
  //   generator.generate_customer();
  // else if (table_info.table_id == SUPP)
  //   generator.generate_supplier();
  // else if (table_info.table_id == NATION)
  //   generator.generate_nation();
  // else if (table_info.table_id == REGION)
  //   generator.generate_region();
  // else
  //   elog(ERROR, "Unsupported table name: %d", table_info.table_id);
}
}
