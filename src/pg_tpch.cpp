
extern "C" {
#include <postgres.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/builtins.h>

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

static int dsdgen_internal(int scale_factor, char* table, int max_row) {
  try {
    return tpch::TPCHWrapper::DSDGen(scale_factor, table, max_row);
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

PG_FUNCTION_INFO_V1(dsdgen_internal);

Datum dsdgen_internal(PG_FUNCTION_ARGS) {
  int sf = PG_GETARG_INT32(0);
  char* table = text_to_cstring(PG_GETARG_TEXT_PP(1));
  int max_row = PG_GETARG_INT32(2);

  int row_count = tpch::dsdgen_internal(sf, table, max_row);

  PG_RETURN_INT32(row_count);
}
}