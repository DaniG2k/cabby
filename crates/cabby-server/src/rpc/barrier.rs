//! Concurrency limit for RPC calls (upstream `rpc/barrier.go`).

use tokio::sync::Semaphore;

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

/// Fixed number of slots (upstream `FixedSizeBarrier`): a fair (FIFO) semaphore.
/// Taking a free slot is a single atomic operation; waiters queue in order.
#[derive(Debug)]
pub struct FixedSizeBarrier {
    capacity: usize,
    capacity_info: String,
    sem: Semaphore,
}

impl FixedSizeBarrier {
    /// Fails unless `capacity > 0`.
    pub fn new(capacity: i64) -> Result<Self, RpcError> {
        let capacity = usize::try_from(capacity)
            .ok()
            .filter(|c| *c > 0 && *c <= Semaphore::MAX_PERMITS)
            .ok_or_else(|| RpcError::Other("RPC concurrency must be > 0".into()))?;
        Ok(Self { capacity, capacity_info: capacity.to_string(), sem: Semaphore::new(capacity) })
    }

    /// Takes a slot, waiting until one is free or `ctx` is done.
    pub async fn wait(&self, ctx: &Context) -> Result<(), RpcError> {
        if let Ok(permit) = self.sem.try_acquire() {
            permit.forget();
            return Ok(());
        }
        tokio::select! {
            permit = self.sem.acquire() => {
                // The semaphore is never closed.
                if let Ok(p) = permit { p.forget() }
                Ok(())
            }
            e = ctx.done() => Err(RpcError::Other(e.to_string())),
        }
    }

    /// Frees a slot taken with [`FixedSizeBarrier::wait`].
    pub fn release(&self) {
        self.sem.add_permits(1);
    }

    /// In-flight calls: the capacity minus the free slots.
    pub fn busy_count(&self) -> usize {
        self.capacity.saturating_sub(self.sem.available_permits())
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn capacity_info(&self) -> String {
        self.capacity_info.clone()
    }

    pub fn exhausted(&self) {}

    pub fn has_dynamic_capacity(&self) -> bool {
        false
    }

    pub fn start(&self) {}

    pub fn stop(&self) {}
}

impl Barrier for FixedSizeBarrier {
    fn acquire<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<(), RpcError>> {
        Box::pin(self.wait(ctx))
    }
    fn release(&self) {
        FixedSizeBarrier::release(self);
    }
    fn busy_count(&self) -> usize {
        FixedSizeBarrier::busy_count(self)
    }
    fn capacity(&self) -> usize {
        FixedSizeBarrier::capacity(self)
    }
    fn capacity_info(&self) -> String {
        FixedSizeBarrier::capacity_info(self)
    }
    fn exhausted(&self) {
        FixedSizeBarrier::exhausted(self);
    }
    fn has_dynamic_capacity(&self) -> bool {
        FixedSizeBarrier::has_dynamic_capacity(self)
    }
    fn start(&self) {
        FixedSizeBarrier::start(self);
    }
    fn stop(&self) {
        FixedSizeBarrier::stop(self);
    }
}
