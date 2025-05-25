pub(crate) struct PGUtils;

impl PGUtils {
    pub unsafe fn heap_tuple_header_set_xmin(
        header: pgrx::pg_sys::HeapTupleHeader,
        xid: pgrx::pg_sys::TransactionId,
    ) {
        (*header).t_choice.t_heap.t_xmin = xid;
    }

    // #define HeapTupleHeaderSetXmax(tup, xid) \
    // ( \
    // 	(tup)->t_choice.t_heap.t_xmax = (xid) \
    // )
    pub unsafe fn heap_tuple_header_set_xmax(
        header: pgrx::pg_sys::HeapTupleHeader,
        xid: pgrx::pg_sys::TransactionId,
    ) {
        (*header).t_choice.t_heap.t_xmax = xid;
    }

    // #define HeapTupleHeaderSetCmin(tup, cid) \
    // do { \
    // 	Assert(!((tup)->t_infomask & HEAP_MOVED)); \
    // 	(tup)->t_choice.t_heap.t_field3.t_cid = (cid); \
    // 	(tup)->t_infomask &= ~HEAP_COMBOCID; \
    // } while (0)
    pub unsafe fn heap_tuple_header_set_cmin(
        header: pgrx::pg_sys::HeapTupleHeader,
        cid: pgrx::pg_sys::CommandId,
    ) {
        (*header).t_choice.t_heap.t_field3.t_cid = cid;
        (*header).t_infomask &= !pgrx::pg_sys::HEAP_COMBOCID as u16;
    }

    pub fn relation_get_target_page_free_space(rel: &pgrx::PgRelation, fill_factor: u32) -> usize {
        let x = if !(*rel).rd_options.is_null() {
            let rf = (*rel).rd_options as *mut pgrx::pg_sys::StdRdOptions;
            (unsafe { *rf }).fillfactor as u32
        } else {
            fill_factor
        };
        (pgrx::pg_sys::BLCKSZ * (100 - x) / 100) as usize
    }

    #[allow(dead_code)]
    fn get_per_tuple_memory_context(
        estate: *mut pgrx::pg_sys::EState,
    ) -> pgrx::pg_sys::MemoryContext {
        unsafe {
            if (*estate).es_per_tuple_exprcontext.is_null() {
                let per_tuple_expr_context = pgrx::pg_sys::MakePerTupleExprContext(estate);
                (*per_tuple_expr_context).ecxt_per_tuple_memory
            } else {
                (*(*estate).es_per_tuple_exprcontext).ecxt_per_tuple_memory
            }
        }
    }
}
