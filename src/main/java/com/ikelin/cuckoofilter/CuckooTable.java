package com.ikelin.cuckoofilter;

import java.util.concurrent.locks.StampedLock;

/**
 * A thread safe cuckoo hash table.
 */
class CuckooTable {

  private final int entriesPerBucket;
  private final int bitsPerEntry;
  private final int bins;
  private final StampedLock[] binLocks;

  // guarded by binLocks
  private final BitSet bitSet;

  /**
   * Creates a cuckoo table.
   *
   * @param buckets number of buckets for this cuckoo table
   * @param entriesPerBucket number of entries per bucket for this cuckoo table
   * @param bitsPerEntry bits per entry for this cuckoo table
   * @param concurrencyLevel concurrency level for this cuckoo table
   */
  CuckooTable(int buckets, int entriesPerBucket, int bitsPerEntry, int concurrencyLevel) {
    if (buckets <= 0) {
      throw new IllegalArgumentException("buckets needs to be greater than 0: " + buckets);
    }

    if (entriesPerBucket <= 0) {
      throw new IllegalArgumentException(
          "entriesPerBucket needs to be greater than 0: " + entriesPerBucket);
    }

    if (bitsPerEntry <= 0) {
      throw new IllegalArgumentException(
          "bitsPerEntry needs to be greater than 0: " + bitsPerEntry);
    }

    if (concurrencyLevel <= 0) {
      throw new IllegalArgumentException(
          "concurrencyLevel needs to be greater than 0: " + concurrencyLevel);
    }

    this.entriesPerBucket = entriesPerBucket;
    this.bitsPerEntry = bitsPerEntry;

    int exp = Integer.SIZE - Integer.numberOfLeadingZeros(concurrencyLevel - 1);
    this.bins = 1 << exp;

    this.binLocks = new StampedLock[this.bins];
    for (int i = 0; i < this.bins; i++) {
      binLocks[i] = new StampedLock();
    }

    this.bitSet = new BitSet((long) buckets * entriesPerBucket * bitsPerEntry);
  }

  /**
   * Returns true if any entries in the {@code bucket} contains {@code value}.
   *
   * @param bucket the bucket to check for value
   * @param value value of bits to check
   * @return true if the provided value exists in the specified bucket
   */
  boolean contains(int bucket, long value) {
    StampedLock lock = getBinLock(bucket);
    long stamp = lock.tryOptimisticRead();
    boolean contains = hasValue(bucket, value);

    if (!lock.validate(stamp)) {
      stamp = lock.readLock();
      try {
        contains = hasValue(bucket, value);
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return contains;
  }

  /**
   * Adds the provided value to the specified bucket.  Returns true if the bucket contains an empty
   * entry and add was successful.  Returns false if the bucket is full and add was not successful.
   *
   * @param bucket the bucket to add value to
   * @param value the value to be added
   */
  boolean addIfEmpty(int bucket, long value) {
    StampedLock lock = getBinLock(bucket);
    long stamp = lock.writeLock();
    try {
      boolean empty = false;
      int entry = 0;
      for (int i = 0; i < entriesPerBucket; i++) {
        if (0 == getValue(bucket, i)) {
          empty = true;
          entry = i;
          break;
        }
      }

      if (!empty) {
        return false;
      }

      orValue(bucket, entry, value);
      return true;
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  /**
   * Gets the current value at the specified bucket and entry, then replaces that value with the
   * provided value.
   *
   * @param bucket the bucket to get and set value
   * @param entry the entry of the bucket to get and set value
   * @param value the value to be set
   * @return the existing value at the specified bucket and entry
   */
  long getAndSet(int bucket, int entry, long value) {
    StampedLock lock = getBinLock(bucket);
    long stamp = lock.writeLock();
    try {
      long existingValue = getValue(bucket, entry);
      if (value == existingValue) {
        return existingValue;
      }

      clearValue(bucket, entry);
      orValue(bucket, entry, value);
      return existingValue;
    } finally {
      lock.unlockWrite(stamp);
    }
  }

  /**
   * Removes the provided value from the specified bucket.  Returns true if the value exists in the
   * bucket and is removed.  Returns false if the value does not exist in the bucket and is not
   * removed.
   *
   * @param bucket the bucket where the value is to be removed
   * @param value the value to be removed
   * @return true if the value was removed
   */
  boolean remove(int bucket, long value) {
    StampedLock lock = getBinLock(bucket);
    long stamp = lock.writeLock();
    try {
      for (int i = 0; i < entriesPerBucket; i++) {
        if (value == getValue(bucket, i)) {
          clearValue(bucket, i);
          return true;
        }
      }
    } finally {
      lock.unlockWrite(stamp);
    }
    return false;
  }

  /**
   * Count the number of times the value appears in the bucket.
   *
   * @param bucket the bucket to be counted
   * @param value the value to be counted
   * @return the number of instances the value appears in the bucket
   */
  int count(int bucket, long value) {
    StampedLock lock = getBinLock(bucket);
    long stamp = lock.tryOptimisticRead();

    int count = 0;
    for (int i = 0; i < entriesPerBucket; i++) {
      if (value == getValue(bucket, i)) {
        count++;
      }
    }

    if (!lock.validate(stamp)) {
      stamp = lock.readLock();
      try {
        count = 0;
        for (int i = 0; i < entriesPerBucket; i++) {
          if (value == getValue(bucket, i)) {
            count++;
          }
        }
      } finally {
        lock.unlockRead(stamp);
      }
    }
    return count;
  }

  private boolean hasValue(int bucket, long value) {
    for (int i = 0; i < entriesPerBucket; i++) {
      if (value == getValue(bucket, i)) {
        return true;
      }
    }
    return false;
  }

  private long getValue(int bucket, int entry) {
    long start = getStartBit(bucket, entry);
    long end = getEndBit(start);
    return bitSet.get(start, end);
  }

  private void orValue(int bucket, int entry, long value) {
    long start = getStartBit(bucket, entry);
    long end = getEndBit(start);
    bitSet.or(start, end, value);
  }

  private void clearValue(int bucket, int entry) {
    long start = getStartBit(bucket, entry);
    long end = getEndBit(start);
    bitSet.clear(start, end);
  }

  private long getStartBit(int bucket, int entry) {
    return ((long) bucket * entriesPerBucket + entry) * bitsPerEntry;
  }

  private long getEndBit(long start) {
    return start + bitsPerEntry;
  }

  private StampedLock getBinLock(int bucket) {
    int bin = bucket % bins;
    return binLocks[bin];
  }

}
