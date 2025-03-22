
#include <cstddef>
#include <exception>
extern "C" {
#include <postgres.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <commands/dbcommands.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <libpq/libpq-be-fe-helpers.h>
#include <libpq/libpq-be.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <postmaster/postmaster.h>
#include <utils/builtins.h>
#include <utils/wait_event.h>

#include <internal/libpq-int.h>
#include <libpq-fe.h>
#include <string.h>
}

#include "tpch/include/tpch_wrapper.hpp"

namespace tpch {

static bool tpch_prepare() {
  try {
    tpch::TPCHWrapper::CreateTPCHSchema();
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to prepare schema, get error: %s", e.what());
  }
  return true;
}

static const char* tpch_queries(int qid) {
  try {
    return tpch::TPCHWrapper::GetQuery(qid);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to get query, get error: %s", e.what());
  }
}

static int tpch_num_queries() {
  return tpch::TPCHWrapper::QueriesCount();
}

static void dbgen_internal(double scale_factor) {
  try {
    tpch::TPCHWrapper::DBGen(scale_factor);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to dsdgen, get error: %s", e.what());
  }
}

static tpch_runner_result* tpch_runner(int qid) {
  try {
    return tpch::TPCHWrapper::RunTPCH(qid);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to run query, get error: %s", e.what());
  }
}

}  // namespace tpch

extern "C" {

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tpch_prepare);
Datum tpch_prepare(PG_FUNCTION_ARGS) {
  bool result = tpch::tpch_prepare();

  PG_RETURN_BOOL(result);
}

/*
 * tpch_queries
 */
PG_FUNCTION_INFO_V1(tpch_queries);

Datum tpch_queries(PG_FUNCTION_ARGS) {
  ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
  Datum values[2];
  bool nulls[2] = {false, false};

  int get_qid = PG_GETARG_INT32(0);
  InitMaterializedSRF(fcinfo, 0);

  if (get_qid == 0) {
    int q_count = tpch::tpch_num_queries();
    int qid = 0;
    while (qid < q_count) {
      const char* query = tpch::tpch_queries(++qid);

      values[0] = qid;
      values[1] = CStringGetTextDatum(query);

      tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }
  } else {
    const char* query = tpch::tpch_queries(get_qid);
    values[0] = get_qid;
    values[1] = CStringGetTextDatum(query);
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
  }
  return 0;
}

PG_FUNCTION_INFO_V1(tpch_runner);

Datum tpch_runner(PG_FUNCTION_ARGS) {
  int qid = PG_GETARG_INT32(0);
  TupleDesc tupdesc;
  Datum values[3];
  bool nulls[3] = {false, false, false};

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    elog(ERROR, "return type must be a row type");

  tpch::tpch_runner_result* result = tpch::tpch_runner(qid);

  values[0] = result->qid;
  values[1] = Float8GetDatum(result->duration);
  values[2] = BoolGetDatum(result->checked);

  PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
sf=1的时候，表数量级如下:
 region   |         5     : 数量固定
 nation   |        25     : 数量固定
 supplier |     10000     : 基数为10000，大小随sf变化
 customer |    150000     : 基数为150000，大小随sf变化
 part     |    200000     : 基数为200000，大小随sf变化
 partsupp |    800000     : 和part为一比四的关系
 orders   |   1500000     : 基数为1500000，大小随sf变化
 lineitem |   6001215     : 和orders为一比四的关系

 任务划分：
  小表不切分，大表按照数据量切分，part和order一比七的关系，最低使用 4 + 1 + 7 = 12 个 worker

 实际调度的时候受到max_parallel_workers的限制，轮询消费，等待空闲

 如果 max_parallel_workers 足够大，则可以多分几个
  简单使用 max(1, max_parallel_workers / 8) 为 part和order 的系数

*/

PG_FUNCTION_INFO_V1(dbgen_internal);
Datum dbgen_internal(PG_FUNCTION_ARGS) {
  double sf = PG_GETARG_FLOAT8(0);

  tpch::dbgen_internal(sf);

  return BoolGetDatum(true);
}
}