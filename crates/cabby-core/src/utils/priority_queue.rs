//! Min-priority queue with addressable items (upstream generic `utils.PriorityQueue`,
//! built on `container/heap`).
//!
//! Upstream hands out item pointers that carry their heap index. Cabby hands out
//! [`ItemId`] handles and keeps a handle-to-index map next to the heap. The sift
//! operations are a port of `container/heap`, so items with equal priorities come out
//! in the same order as upstream. Updating or removing an item that is no longer in
//! the queue is a no-op (upstream corrupts the heap or panics).

use std::collections::HashMap;

/// Handle of an item pushed into a [`PriorityQueue`]. Upstream returns item
/// pointers and tests compare them by identity; handles compare the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ItemId(pub u64);

/// A queued item (upstream `PriorityQueueItem`).
#[derive(Debug, Clone, PartialEq)]
pub struct PriorityQueueItem<T, P> {
    pub id: ItemId,
    pub value: T,
    pub priority: P,
}

/// Min-priority queue (upstream generic `utils.PriorityQueue`).
#[derive(Debug)]
pub struct PriorityQueue<T, P> {
    heap: Vec<PriorityQueueItem<T, P>>,
    index: HashMap<ItemId, usize>,
    next_id: u64,
}

impl<T: Clone, P: PartialOrd + Clone> PriorityQueue<T, P> {
    pub fn new() -> Self {
        PriorityQueue { heap: Vec::new(), index: HashMap::new(), next_id: 1 }
    }

    pub fn push_item(&mut self, value: T, priority: P) -> ItemId {
        let id = ItemId(self.next_id);
        self.next_id += 1;
        let n = self.heap.len();
        self.heap.push(PriorityQueueItem { id, value, priority });
        self.index.insert(id, n);
        self.up(n);
        id
    }

    /// Removes and returns the lowest-priority item.
    pub fn pop_item(&mut self) -> Option<PriorityQueueItem<T, P>> {
        let n = self.heap.len().checked_sub(1)?;
        self.swap(0, n);
        self.down(0, n);
        self.take_last()
    }

    /// Changes the priority of an item still in the queue.
    pub fn update(&mut self, item: ItemId, priority: P) {
        let Some(&i) = self.index.get(&item) else { return };
        self.heap[i].priority = priority;
        // heap.Fix
        if !self.down(i, self.heap.len()) {
            self.up(i);
        }
    }

    /// The lowest-priority item, if any.
    pub fn peek(&self) -> Option<PriorityQueueItem<T, P>> {
        self.heap.first().cloned()
    }

    pub fn remove(&mut self, item: ItemId) {
        let Some(&i) = self.index.get(&item) else { return };
        // heap.Remove
        let n = self.heap.len() - 1;
        if n != i {
            self.swap(i, n);
            if !self.down(i, n) {
                self.up(i);
            }
        }
        self.take_last();
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn take_last(&mut self) -> Option<PriorityQueueItem<T, P>> {
        let item = self.heap.pop()?;
        self.index.remove(&item.id);
        Some(item)
    }

    fn less(&self, i: usize, j: usize) -> bool {
        self.heap[i].priority < self.heap[j].priority
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.heap.swap(i, j);
        self.index.insert(self.heap[i].id, i);
        self.index.insert(self.heap[j].id, j);
    }

    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            j = i;
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2;
            }
            if !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            i = j;
        }
        i > i0
    }
}

impl<T: Clone, P: PartialOrd + Clone> Default for PriorityQueue<T, P> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pops_in_priority_order_after_updates_and_removals() {
        let mut pq = PriorityQueue::new();
        let ids: Vec<_> = (0..100u64).map(|i| pq.push_item(i, (i * 37) % 101)).collect();
        for (k, id) in ids.iter().enumerate().step_by(3) {
            pq.update(*id, (k as u64 * 11) % 97);
        }
        for id in ids.iter().skip(1).step_by(5) {
            pq.remove(*id);
        }
        pq.remove(ids[1]); // already removed: no-op
        let mut last = 0;
        let mut count = 0;
        while let Some(item) = pq.pop_item() {
            assert!(item.priority >= last);
            last = item.priority;
            count += 1;
        }
        assert_eq!(80, count);
        assert!(pq.pop_item().is_none());
        assert!(pq.peek().is_none());
    }
}
