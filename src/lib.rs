use std::thread::sleep;

use pgrx::{
    bgworkers::BackgroundWorkerBuilder,
    pg_sys::{FmgrInfo, Oid},
    prelude::*,
    PgRelation,
};

mod functions;
mod workers;

::pgrx::pg_module_magic!();

// TODO: 模拟多个worker并发执行insert 操作，看看是否会冲突
pub fn from_oid(typoid: Oid) -> (FmgrInfo, Oid) {
    let mut type_input = pg_sys::Oid::INVALID;
    let mut typ_io_param = pg_sys::oids::Oid::INVALID;
    let mut flinfo = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
    unsafe {
        pg_sys::getTypeInputInfo(typoid, &mut type_input, &mut typ_io_param);
        pg_sys::fmgr_info(type_input, &mut flinfo);
    }

    (flinfo, typ_io_param)
}

fn test_insert() {
    let table = unsafe { PgRelation::open_with_name("t1").expect("table not found") };
    let tupledesc = table.tuple_desc();
    for desc in tupledesc.iter() {
        let (flinfo, typ_io_param) = from_oid(desc.type_oid().value());
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn test_bgworker(arg: pg_sys::Datum) {
    use pgrx::bgworkers::*;
    use std::time::Duration;
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let arg = unsafe { i32::from_datum(arg, false) }.expect("invalid arg");

    test_insert();

    sleep(Duration::from_secs(arg.try_into().unwrap()));
    for i in 0..arg {
        println!("bgworker {} running", i)
    }
}

#[pg_extern]
fn test_dy_worker(num: i32) {
    let mut worker_arrays = vec![];
    for i in 0..num {
        let worke = BackgroundWorkerBuilder::new("dynamic_bgworker")
            .set_library("pg_tpchrs")
            .set_function("test_bgworker")
            .set_argument(i.into_datum())
            .enable_shmem_access(None)
            .set_notify_pid(unsafe { pg_sys::MyProcPid })
            .load_dynamic()
            .expect("Failed to start worker");
        let pid = worke.wait_for_startup().expect("Failed to start worker");

        info!("Worker {} started with pid {}", i, pid);
        worker_arrays.push(worke);
    }
    for worker in worker_arrays {
        worker
            .wait_for_shutdown()
            .expect("Failed to shutdown worker");
    }
}
#[pg_extern]
fn hello_pg_tpchrs() -> &'static str {
    "Hello, pg_tpchrs"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_tpchrs() {
        assert_eq!("Hello, pg_tpchrs", crate::hello_pg_tpchrs());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
