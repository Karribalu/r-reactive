use crate::dash::pair::{Pair, ValueT};
use std::fmt::Debug;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::Arc;
use thiserror::Error;

const LOCK_SET: u32 = 1 << 31;
const LOCK_MASK: u32 = (1 << 31) - 1;
pub(crate) const K_NUM_PAIR_PER_BUCKET: usize = 14; // Number of pairs in a bucket

const COUNT_MASK: u32 = (1 << 4) - 1;

#[derive(Debug, Error)]
pub(crate) enum BucketError {
    #[error("Bucket is full")]
    BucketFull,
}
#[derive(Debug, Clone)]
pub(crate) struct Bucket<T: PartialEq + Clone> {
    pub(crate) pairs: Vec<Option<Pair<T>>>,
    pub(crate) overflow_count: u8,
    pub(crate) overflow_member: u8, // Used to store any overflow member from b-1 bucket
    pub(crate) overflow_index: u8,
    pub(crate) overflow_bitmap: u8, // Overflow member is used to identify if any items stored in this stash bucket from the target bucket
    pub(crate) fingerprints: [u8; 18], // only use the first 14 bytes, can be speeded up by SSE instruction,0-13 for finger, 14-17 for overflowed
    pub(crate) bitmap: u32,            // allocation bitmap + pointer bitmap + counter
    pub(crate) version_lock: Arc<AtomicU32>,
}

/**
for Bitmap: 32 bits
0000 0000 1110 00 0000 00 0000 0000 0101
First 14 bits are for allocating the buckets
Next 4 is for stash buckets
Next 10 is for pointers
The last 4 bits determine the number of slots filled in the bucket,
In the above example 5 slots are filled
*/
impl<T: Debug + Clone + PartialEq> Bucket<T> {
    pub(crate) fn new() -> Self {
        Bucket {
            pairs: vec![None; K_NUM_PAIR_PER_BUCKET],
            overflow_count: 0,
            overflow_member: 0,
            overflow_index: 0,
            overflow_bitmap: 0,
            fingerprints: [0; 18],
            bitmap: 0,
            version_lock: Arc::new(AtomicU32::new(0)),
        }
    }
    /**
     * This function is used to get the lock for the bucket
     * It will keep trying to get the lock until it succeeds,
     */
    pub(crate) fn get_lock(&self) {
        loop {
            let old_value = self.version_lock.load(Acquire) & LOCK_MASK;
            if self
                .version_lock
                .compare_exchange(old_value, old_value | LOCK_SET, Acquire, Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    /**
     * This function is used to release the lock for the bucket
     * It will set the lock bit to 0
     */
    pub(crate) fn release_lock(&self) {
        let old_value = self.version_lock.load(Acquire);
        self.version_lock.store(old_value + 1 - LOCK_SET, Release);
    }

    pub(crate) fn reset_lock(&self) {
        self.version_lock.store(0, SeqCst);
    }

    pub(crate) fn try_get_lock(&self) -> bool {
        let old_value = self.version_lock.load(Acquire) & LOCK_MASK;
        self.version_lock
            .compare_exchange(old_value, old_value | LOCK_SET, Acquire, Acquire)
            .is_ok()
    }

    pub(crate) fn is_locked(&self) -> bool {
        self.version_lock.load(Acquire) & LOCK_SET != 0
    }

    // FIXME: Do we need the slot to be returned?
    pub(crate) fn insert(
        &mut self,
        key: &T,
        value: ValueT,
        meta_hash: u8,
        probe: bool,
    ) -> Result<usize, BucketError> {
        if let Some(slot) = self.find_empty_slot() {
            self.pairs[slot] = Some(Pair::new(key.clone(), value));
            self.set_hash(slot, meta_hash, probe);
            Ok(slot)
        } else {
            Err(BucketError::BucketFull)
        }
    }

    fn find_empty_slot(&self) -> Option<usize> {
        if get_count_from_bitmap(self.bitmap) == K_NUM_PAIR_PER_BUCKET {
            return None;
        }

        let mask = !get_bitmap(self.bitmap);
        Some(mask.trailing_zeros() as usize)
    }

    fn set_hash(&mut self, slot: usize, hash: u8, probe: bool) {
        self.fingerprints[slot] = hash;
        let mut new_bitmap = self.bitmap | (1 << (slot + 18));
        if probe {
            // Meaning the value is being hosted but not owned by this bucket
            new_bitmap |= 1 << (slot + 4);
        }
        new_bitmap += 1;
        self.bitmap = new_bitmap;
    }
}

/**
0000 0000 0001 1111 0000 0000 0000 0101 & 0000 0000 0000 0000 0000 0000 0000 1111
it returns 5 as the count
*/
fn get_count_from_bitmap(map: u32) -> usize {
    let count = map & COUNT_MASK;
    count as usize
}

/**
We remove last 4 bits which are for count
and 14 bits before that which are for pointers
*/
pub fn get_bitmap(var: u32) -> u32 {
    var >> 18
}
