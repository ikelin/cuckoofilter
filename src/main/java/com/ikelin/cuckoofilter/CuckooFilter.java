package com.ikelin.cuckoofilter;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread safe probability filter that supports put, mightContain, remove and count.
 */
public class CuckooFilter {

  private final int buckets;
  private final int bitsPerEntry;
  private final int entriesPerBucket;
  private final int concurrencyLevel;
  private final int maxRelocateAttempts;

  private final CuckooTable table;
  private final AtomicInteger items;
  private final int unusedBits;

  private CuckooFilter(
      final int buckets,
      final int entriesPerBucket,
      final int bitsPerEntry,
      final int concurrencyLevel,
      final int maxRelocateAttempts
  ) {
    this.buckets = buckets;
    this.entriesPerBucket = entriesPerBucket;
    this.bitsPerEntry = bitsPerEntry;
    this.concurrencyLevel = concurrencyLevel;
    this.maxRelocateAttempts = maxRelocateAttempts;

    this.table = new CuckooTable(buckets, entriesPerBucket, bitsPerEntry, concurrencyLevel);
    this.items = new AtomicInteger();
    this.unusedBits = Long.SIZE - bitsPerEntry;
  }

  /**
   * Creates a builder of {@link CuckooFilter} instances.
   *
   * @param expectedMaxCapacity the expected maximum number of items in the filter
   * @throws IllegalArgumentException if {@code expectedMaxCapacity} is not greater than 0
   */
  public static Builder create(final int expectedMaxCapacity) {
    return new Builder(expectedMaxCapacity);
  }

  /**
   * Checks if the {@code itemHash} is in this filter.  Returns true if {@code itemHash}
   * <b>might be</b> in this filter. Returns false if {@code itemHash} is <b>definitely not</b>
   * in this filter.
   *
   * @param itemHash the hash value of the item to lookup
   * @return true if hash value of the item might be in this filter.  Returns false if hash value of
   *     the item is definitely not in this filter.
   */
  public boolean mightContain(final long itemHash) {
    long fingerprint = getFingerprint(itemHash);

    int bucket = getBucket(itemHash);
    if (table.contains(bucket, fingerprint)) {
      return true;
    }

    int altBucket = getBucket(bucket, fingerprint);
    return table.contains(altBucket, fingerprint);
  }

  /**
   * Puts the {@code itemHash} into this filter.  Returns true if {@code itemHash} is added
   * successfully.  Returns false if the buckets are full.
   *
   * @param itemHash the hash value of the item to add
   * @return true if hash value of the item is added
   */
  public boolean put(final long itemHash) {
    long fingerprint = getFingerprint(itemHash);

    int bucket = getBucket(itemHash);
    if (table.addIfEmpty(bucket, fingerprint)) {
      items.incrementAndGet();
      return true;
    }

    int altBucket = getBucket(bucket, fingerprint);
    if (table.addIfEmpty(altBucket, fingerprint)) {
      items.incrementAndGet();
      return true;
    }

    if (ThreadLocalRandom.current().nextBoolean()) {
      bucket = altBucket;
    }

    for (int i = 0; i < maxRelocateAttempts; i++) {
      int entry = ThreadLocalRandom.current().nextInt(entriesPerBucket);
      fingerprint = table.getAndSet(bucket, entry, fingerprint);
      bucket = getBucket(bucket, fingerprint);
      if (table.addIfEmpty(bucket, fingerprint)) {
        items.incrementAndGet();
        return true;
      }
    }
    return false;
  }

  /**
   * Removes an {@code itemHash} from this filter.  Returns true if {@code itemHash} is removed
   * successfully.  Returns false if {@code itemHash} is not found in this filter.
   *
   * @param itemHash the hash value of the item to remove
   * @return true if hash value of the item is removed
   */
  public boolean remove(final long itemHash) {
    long fingerprint = getFingerprint(itemHash);

    int bucket = getBucket(itemHash);
    if (table.remove(bucket, fingerprint)) {
      items.decrementAndGet();
      return true;
    }

    int altBucket = getBucket(bucket, fingerprint);
    if (table.remove(altBucket, fingerprint)) {
      items.decrementAndGet();
      return true;
    }

    return false;
  }

  /**
   * Counts the number of times {@code itemHash} is in this filter.
   *
   * @param itemHash the hash value of the item to count
   * @return the number of times the hash value of the item is in this filter
   */
  public int count(final long itemHash) {
    long fingerprint = getFingerprint(itemHash);

    int count = 0;
    int bucket = getBucket(itemHash);
    count += table.count(bucket, fingerprint);

    int altBucket = getBucket(bucket, fingerprint);
    if (bucket != altBucket) {
      count += table.count(altBucket, fingerprint);
    }

    return count;
  }

  /**
   * Returns the current number of items in this filter.
   *
   * @return the current number of items in this filter
   */
  public int getItems() {
    return items.get();
  }

  /**
   * Returns the current load factor of this filter.
   *
   * @return the current load factor
   */
  public double getLoadFactor() {
    return (double) getItems() / (getBuckets() * getEntriesPerBucket());
  }

  /**
   * Returns the capacity of this filter.
   *
   * @return the capacity of this filter
   */
  protected int getCapacity() {
    return getBuckets() * getEntriesPerBucket();
  }

  /**
   * Returns the number of buckets in this filter.
   *
   * @return the number of buckets in this filter.
   */
  protected int getBuckets() {
    return buckets;
  }

  /**
   * Returns the number of entries per bucket of this filter.
   *
   * @return the number of entries per bucket of this filter
   */
  protected int getEntriesPerBucket() {
    return entriesPerBucket;
  }

  /**
   * Returns the number of bits per entry of this filter.
   *
   * @return the number of bits per entry of this filter.
   */
  protected int getBitsPerEntry() {
    return bitsPerEntry;
  }

  /**
   * Returns the concurrency level of this filter.
   *
   * @return the concurrency level of this filter.
   */
  protected int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  private long getFingerprint(final long itemHash) {
    for (int i = 0; i < Long.SIZE / bitsPerEntry; i++) {
      long fingerprint = itemHash << (Long.SIZE - bitsPerEntry * i) >>> unusedBits;
      if (fingerprint != 0) {  // fingerprint cannot be the same as an empty entry
        return fingerprint;
      }
    }
    return 1L;
  }

  private int getBucket(final long itemHash) {
    return getBucketIndex(itemHash >> bitsPerEntry);
  }

  private int getBucket(final int bucket, final long fingerprint) {
    return getBucketIndex(bucket ^ (fingerprint * 0x5bd1e995));
  }

  private int getBucketIndex(final long bucketHash) {
    long hash;
    if (bucketHash < 0) {
      hash = ~bucketHash;
    } else {
      hash = bucketHash;
    }

    return (int) (hash & (buckets - 1));  // hash % buckets
  }

  @Override
  public String toString() {
    return "CuckooFilter{"
        + "buckets=" + buckets
        + ", entriesPerBucket=" + entriesPerBucket
        + ", bitsPerEntry=" + bitsPerEntry
        + ", maxRelocateAttempts=" + maxRelocateAttempts
        + '}';
  }

  /**
   * A builder of {@link CuckooFilter} instances.
   */
  public static class Builder {

    private final int expectedMaxCapacity;
    private double fpp;
    private int bitsPerEntry;
    private int entriesPerBucket;
    private int concurrencyLevel;

    private Builder(final int expectedMaxCapacity) {
      if (expectedMaxCapacity < 1) {
        throw new IllegalArgumentException("expectedMaxCapacity must be at least 1");
      }

      this.expectedMaxCapacity = expectedMaxCapacity;
      this.fpp = 0.002D;
    }

    /**
     * Sets the false positive probability for the filter.  A value of 0.03 means 3% false positive
     * probability. Valid {@code fpp} is between 0 (exclusive) and 1 (exclusive).  Defaults to 0.002
     * (0.2%) if not specified.
     *
     * @param fpp the false positive probability for the filter
     * @return the builder instance
     * @throws IllegalArgumentException if {@code fpp} is not a between 0 (exclusive) and 1
     *     (exclusive)
     */
    public Builder withFalsePositiveProbability(final double fpp) {
      if (fpp <= 0 || fpp >= 1) {
        throw new IllegalArgumentException("expectedMaxCapacity must be between 0 and 1");
      }

      this.fpp = fpp;
      return this;
    }

    /**
     * Sets the bits per entry for the filter.  Typically, bits per entry is determined by the false
     * positive probability of the filter, this method overrides that value with the provided {@code
     * bitsPerEntry}.  Changing this impacts the false positive probability of the filter.
     *
     * @param bitsPerEntry number of bits per entry
     * @return the builder instance
     * @throws IllegalArgumentException if {@code bitsPerEntry} is not between 1 and {@link
     *     java.lang.Integer#SIZE}
     */
    public Builder withBitsPerEntry(final int bitsPerEntry) {
      if (bitsPerEntry <= 0 || bitsPerEntry >= Integer.SIZE) {
        throw new IllegalArgumentException("bitsPerEntry must be between 1 and " + Integer.SIZE);
      }

      this.bitsPerEntry = bitsPerEntry;
      return this;
    }

    /**
     * Sets the entries per bucket for the filter.  Typically, entries per bucket is determined by
     * the false positive probability of the filter.  This method overrides that value with the
     * provided {@code entriesPerBucket}.  Changing this impacts the number of buckets of the
     * filter.  Valid {@code entriesPerBucket} must be a power of 2 (1, 2, 4, 8).
     *
     * @param entriesPerBucket number of entries per bucket.
     * @return the builder instance
     * @throws IllegalArgumentException if {@code entriesPerBucket} is not power of 2 (1, 2, 4,
     *     8...)
     */
    public Builder withEntriesPerBucket(final int entriesPerBucket) {
      if (entriesPerBucket < 1) {
        throw new IllegalArgumentException("entriesPerBucket must be at least 1");
      }

      if (entriesPerBucket > 8) {
        throw new IllegalArgumentException("entriesPerBucket must be at most 8");
      }

      boolean powerOfTwo = (int) Math.ceil(Math.log(entriesPerBucket) / Math.log(2))
          == (int) Math.floor(Math.log(entriesPerBucket) / Math.log(2));
      if (!powerOfTwo) {
        throw new IllegalArgumentException("entriesPerBucket must be power of 2");
      }

      this.entriesPerBucket = entriesPerBucket;
      return this;
    }

    /**
     * Sets the concurrency level for the filter.  Defaults to the number of processors.
     *
     * @param concurrencyLevel the expected number of threads to be accessing this filter
     * @return the builder instance
     * @throws IllegalArgumentException if {@code concurrencyLevel} is not at least 1
     */
    public Builder withConcurrencyLevel(final int concurrencyLevel) {
      if (concurrencyLevel < 1) {
        throw new IllegalArgumentException("concurrencyLevel must be at least 1");
      }

      this.concurrencyLevel = concurrencyLevel;
      return this;
    }

    /**
     * Builds a {@link CuckooFilter} instance.
     *
     * @return a {@link CuckooFilter} instance with the requested settings
     */
    public CuckooFilter build() {
      if (entriesPerBucket == 0) {
        entriesPerBucket = getEntriesPerBucket(fpp);
      }

      double loadFactor = getLoadFactor(entriesPerBucket);

      if (bitsPerEntry == 0) {
        bitsPerEntry = getBitsPerEntry(fpp, loadFactor);
      }

      int buckets = getBuckets(expectedMaxCapacity, entriesPerBucket, loadFactor);

      if (concurrencyLevel == 0) {
        concurrencyLevel = getConcurrencyLevel(buckets);
      }

      int maxRelocateAttempts = getMaxRelocateAttempts(buckets);

      return new CuckooFilter(
          buckets,
          entriesPerBucket,
          bitsPerEntry,
          concurrencyLevel,
          maxRelocateAttempts
      );
    }

    private int getEntriesPerBucket(final double fpp) {
      int entriesPerBucket;
      if (fpp < 0.00001D) {
        entriesPerBucket = 8;
      } else if (fpp <= 0.002D) {
        entriesPerBucket = 4;
      } else {
        entriesPerBucket = 2;
      }
      return entriesPerBucket;
    }

    private double getLoadFactor(final int entriesPerBucket) {
      double loadFactor;
      switch (entriesPerBucket) {
        case 8:
          loadFactor = 0.98D;
          break;
        case 4:
          loadFactor = 0.955D;
          break;
        case 2:
        default:
          loadFactor = 0.84D;
          break;
      }
      return loadFactor;
    }

    private int getBitsPerEntry(final double fpp, final double loadFactor) {
      return (int) Math.ceil((Math.log(1 / fpp) / Math.log(2) + 3) / loadFactor);
    }

    private int getBuckets(final int expectedMaxCapacity, final int entriesPerBucket,
        final double loadFactor) {
      int buckets = (int) Math.ceil(expectedMaxCapacity / entriesPerBucket / loadFactor);
      int exp = Integer.SIZE - Integer.numberOfLeadingZeros(buckets - 1);
      return 1 << exp;
    }

    private int getConcurrencyLevel(final int buckets) {
      int concurrencyLevel = Runtime.getRuntime().availableProcessors();
      if (concurrencyLevel >= buckets) {
        concurrencyLevel = buckets;
      }
      return concurrencyLevel;
    }

    private int getMaxRelocateAttempts(final int buckets) {
      return Math.min(buckets, 500);
    }

  }

}
