//! Concurrency limit for RPC calls (upstream `rpc/barrier.go`).

use cabby_core::pending;

use super::{BoxFuture, Context, RpcError};

/// Limits concurrent RPC calls. Implementations may adapt their capacity
/// (`exhausted` is called on ResourceExhausted responses).
pub trait Barrier: Send + Sync {
    /// Waits for a free slot or until `ctx` is cancelled / past its deadline.
    fn acquire<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<(), RpcError>>;
    fn release(&self);
    fn busy_count(&self) -> usize;
    fn capacity(&self) -> usize;
    fn capacity_info(&self) -> String;
    fn exhausted(&self);
    fn has_dynamic_capacity(&self) -> bool;
    fn start(&self);
    fn stop(&self);
}

/// Fixed number of slots (upstream `FixedSizeBarrier`).
#[derive(Debug)]
pub struct FixedSizeBarrier {
    _capacity: i64,
}

impl FixedSizeBarrier {
    /// Fails unless `capacity > 0`.
    pub fn new(_capacity: i64) -> Result<Self, RpcError> {
        pending!("rpc::new_fixed_size_barrier")
    }
}

impl Barrier for FixedSizeBarrier {
    fn acquire<'a>(&'a self, _ctx: &'a Context) -> BoxFuture<'a, Result<(), RpcError>> {
        pending!("rpc::barrier_acquire")
    }
    fn release(&self) {
        pending!("rpc::barrier_release")
    }
    fn busy_count(&self) -> usize {
        pending!("rpc::barrier_busy_count")
    }
    fn capacity(&self) -> usize {
        pending!("rpc::barrier_capacity")
    }
    fn capacity_info(&self) -> String {
        pending!("rpc::barrier_capacity_info")
    }
    fn exhausted(&self) {
        pending!("rpc::barrier_exhausted")
    }
    fn has_dynamic_capacity(&self) -> bool {
        pending!("rpc::barrier_has_dynamic_capacity")
    }
    fn start(&self) {
        pending!("rpc::barrier_start")
    }
    fn stop(&self) {
        pending!("rpc::barrier_stop")
    }
}
