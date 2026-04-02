# Lineitem Heap Optimization Notes

Updated: 2026-04-02

## Context

Current stable benchmark trend before this parallel experiment:

- Original baseline:
  - `select generate_lineitem(10);`
  - `(59986052,664789.702442,112146.407561)`
- After internal-format datum encoding:
  - heap load dropped from ~665s to ~60s
- Current stable serial path before parallel page-build attempt:
  - `select generate_lineitem(10);`
  - `(59986052,61384.644756999995,104325.761062)`
  - Total time: `166525.971 ms`

Conclusion so far:

- Heap is already much faster than the original version.
- Index build is still large, but current focus is heap.
- The previous scratch-tuple experiment was a negative optimization and was removed.

## Stable Changes Already Kept

These are part of the current serial path and were validated:

- Direct encoding of `text/date/numeric` into PostgreSQL internal format
- Cached relation metadata in loader:
  - `tuple_desc`
  - relation path
  - target page free space
  - checksum enabled flag
- Larger heap page buffer based on machine memory
- `MemoryContextSwitchTo()` moved from per-row to per-batch

Validation already passed for this stable path:

- `cargo fmt`
- `cargo check`
- `cargo pgrx test`
- Small-table regression at `sf=0.01`

## Current Parallel Experiment

Goal:

- Try a `lineitem`-specific parallel heap loader
- Worker threads:
  - generate `LineItem` rows using `tpchgen` `part/part_count`
  - build tuple bytes
  - build full heap pages
- Main thread:
  - patch `ctid`
  - apply checksum
  - write relfile
  - do the existing `REINDEX TABLE`

New SQL entry added:

- `generate_lineitem_parallel(scale_factor, workers)`

Main files involved:

- `src/lib.rs`
- `src/loader.rs`
- `src/encoding.rs`

## Important Discovery

`pgrx` rejects PostgreSQL FFI calls from worker threads, even for low-level page helpers.

What failed:

- Calling PostgreSQL page helpers such as:
  - `PageInit`
  - `PageGetFreeSpace`
  - `PageAddItemExtended`
  - `ItemPointerSetInvalid`

Observed runtime error:

- `postgres FFI may not be called from multiple threads`

So the worker path had to be rewritten to use pure Rust page construction instead of PostgreSQL FFI.

## Current In-Progress State

Current code now does this for the parallel path:

- Worker threads no longer use PostgreSQL page FFI
- Workers build pages with pure Rust page-header and item-pointer manipulation
- Main thread still uses PostgreSQL APIs for:
  - patching `ctid`
  - `log_newpage`
  - checksum
  - relfile writing
  - relcache update
  - reindex

There was one confirmed bug in the parallel path:

- Block 0 became invalid during readback
- Most likely cause:
  - checksum was being applied before `log_newpage`
  - if `log_newpage` updates page header LSN, the first page checksum becomes stale

I have already patched the code to move checksum application after `log_newpage`, matching the serial loader’s ordering.

Important:

- That latest checksum-order fix has **not** been re-validated yet.
- At the moment, the branch should be considered **in-progress**, not stable.

## Last Verified Test State

Before the latest checksum-order patch:

- `cargo check` passed
- `cargo pgrx test` failed
- Failure:
  - `test_generate_lineitem_parallel_loads_numeric_and_date_columns`
  - PostgreSQL error:
    - `invalid page in block 0 of relation ...`

This strongly suggests the parallel data path is close enough to write pages, but page correctness is not yet fully settled.

## Next Plan

When resuming, the next steps should be:

1. Re-run:
   - `cargo check`
   - `cargo pgrx test`
2. Verify whether moving checksum after `log_newpage` fixes block 0 corruption.
3. If the parallel test still fails:
   - compare first generated page from parallel path vs serial path
   - inspect:
     - `pd_lower`
     - `pd_upper`
     - `pd_pagesize_version`
     - item pointer offsets/lengths
     - tuple header `t_hoff`
     - first tuple `t_infomask/t_infomask2`
4. Once correctness passes on `sf=0.01`:
   - benchmark:
     - `select generate_lineitem_parallel(10, 2);`
     - `select generate_lineitem_parallel(10, 4);`
     - compare with serial `generate_lineitem(10)`
5. If parallel path is actually faster and stable:
   - decide whether to keep it as:
     - a separate experimental entry, or
     - the implementation behind `generate_lineitem()`

## Notes For Resume

- Do not revisit the old scratch-tuple path; it was slower.
- Do not spend more time on `drop/create index`; it did not help.
- The most promising direction remains:
  - pure Rust worker-thread page construction
  - main-thread relfile writing

