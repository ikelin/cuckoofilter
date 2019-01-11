package com.ikelin.cuckoofilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CuckooFilterTest {

  private static LongHashFunction hashFunction;
  private static long fooHash;
  private static long barHash;

  @BeforeAll
  static void beforeAll() {
    hashFunction = LongHashFunction.xx();
    fooHash = hashFunction.hashChars("foo");
    barHash = hashFunction.hashChars("bar");
  }

  @Test
  void testCreateWithDefault() {
    CuckooFilter filter = CuckooFilter.create(100).build();
    assertEquals(128, filter.getCapacity());
    assertEquals(32, filter.getBuckets());
    assertEquals(4, filter.getEntriesPerBucket());
    assertEquals(13, filter.getBitsPerEntry());
  }

  @Test
  void testCreateWithFpp() {
    CuckooFilter filter = CuckooFilter.create(100).withFalsePositiveProbability(0.001D).build();
    assertEquals(32, filter.getBuckets());
    assertEquals(4, filter.getEntriesPerBucket());
    assertEquals(14, filter.getBitsPerEntry());
  }

  @Test
  void testCreateWithBitsPerEntry() {
    CuckooFilter filter = CuckooFilter.create(100).withBitsPerEntry(8).build();
    assertEquals(32, filter.getBuckets());
    assertEquals(4, filter.getEntriesPerBucket());
    assertEquals(8, filter.getBitsPerEntry());
  }

  @Test
  void testCreateWithEntriesPerBucket() {
    CuckooFilter filter = CuckooFilter.create(100).withEntriesPerBucket(8).build();
    assertEquals(16, filter.getBuckets());
    assertEquals(8, filter.getEntriesPerBucket());
    assertEquals(13, filter.getBitsPerEntry());
  }

  @Test
  void testCreateWithConcurrencyLevel() {
    CuckooFilter filter = CuckooFilter.create(100).withConcurrencyLevel(3).build();
    assertEquals(3, filter.getConcurrencyLevel());
  }

  @Test
  void testPut() {
    CuckooFilter filter = CuckooFilter.create(100).build();
    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(barHash));
    assertTrue(filter.mightContain(fooHash));
    assertTrue(filter.mightContain(barHash));
    assertEquals(1, filter.count(fooHash));
    assertEquals(1, filter.count(fooHash));
  }

  @Test
  void testPutDuplicate() {
    int entriesPerBucket = 4;
    CuckooFilter filter = CuckooFilter.create(100).withEntriesPerBucket(entriesPerBucket).build();

    for (int i = 0; i < entriesPerBucket * 2; i++) {
      assertTrue(filter.put(fooHash));
      assertEquals(i + 1, filter.count(fooHash));
    }

    assertFalse(filter.put(fooHash));
    assertEquals(entriesPerBucket * 2, filter.count(fooHash));
  }

  @Test
  void testMightContain() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertTrue(filter.put(fooHash));
    assertTrue(filter.mightContain(fooHash));
    assertFalse(filter.mightContain(barHash));
    assertEquals(1, filter.count(fooHash));
  }

  @Test
  void testMightContainDuplicate() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(fooHash));
    assertTrue(filter.mightContain(fooHash));
    assertEquals(2, filter.count(fooHash));
  }

  @Test
  void testRemove() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(barHash));
    assertTrue(filter.mightContain(fooHash));
    assertTrue(filter.mightContain(barHash));
    assertTrue(filter.remove(fooHash));
    assertTrue(filter.remove(barHash));
    assertTrue(filter.mightContain(fooHash));
    assertFalse(filter.mightContain(barHash));
    assertEquals(1, filter.count(fooHash));
    assertEquals(0, filter.count(barHash));
    assertEquals(1, filter.getItems());
  }

  @Test
  void testRemoveNonExistingItem() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertFalse(filter.remove(fooHash));
    assertFalse(filter.remove(barHash));
  }

  @Test
  void testRemoveDuplicate() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(fooHash));
    assertEquals(2, filter.count(fooHash));

    assertTrue(filter.remove(fooHash));
    assertTrue(filter.mightContain(fooHash));
    assertEquals(1, filter.count(fooHash));

    assertTrue(filter.remove(fooHash));
    assertFalse(filter.mightContain(fooHash));
    assertEquals(0, filter.count(fooHash));
  }

  @Test
  void testCount() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertEquals(0, filter.count(fooHash));
    assertEquals(0, filter.count(barHash));

    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(barHash));
    assertEquals(1, filter.count(fooHash));
    assertEquals(1, filter.count(barHash));

    assertTrue(filter.put(fooHash));
    assertTrue(filter.put(barHash));
    assertEquals(2, filter.count(fooHash));
    assertEquals(2, filter.count(barHash));

    assertTrue(filter.remove(fooHash));
    assertTrue(filter.remove(barHash));
    assertEquals(1, filter.count(fooHash));
    assertEquals(1, filter.count(barHash));

    assertTrue(filter.remove(fooHash));
    assertTrue(filter.remove(barHash));
    assertEquals(0, filter.count(fooHash));
    assertEquals(0, filter.count(barHash));
  }

  @Test
  void testGetItems() {
    CuckooFilter filter = CuckooFilter.create(100).build();

    assertTrue(filter.put(fooHash));
    assertEquals(1, filter.getItems());

    assertTrue(filter.put(barHash));
    assertEquals(2, filter.getItems());

    assertTrue(filter.put(fooHash));
    assertEquals(3, filter.getItems());

    assertTrue(filter.remove(fooHash));
    assertEquals(2, filter.getItems());

    assertTrue(filter.remove(barHash));
    assertEquals(1, filter.getItems());

    assertTrue(filter.remove(fooHash));
    assertEquals(0, filter.getItems());

  }

  @Test
  void testConcurrency() throws ExecutionException, InterruptedException {
    int capacity = (int) ((1 << 10) * 0.955D);
    int items = (int) (capacity * 0.955D);
    double fpp = 0.002D;

    CuckooFilter filter = CuckooFilter.create(capacity)
        .withFalsePositiveProbability(fpp)
        .withConcurrencyLevel(8)
        .build();

    ExecutorService executorService = Executors.newFixedThreadPool(8);

    // ids to put into filter
    List<Long> ids = new ArrayList<>(items);
    for (int i = 0; i < items; i++) {
      ids.add(hashFunction.hashChars(UUID.randomUUID().toString()));
    }

    // put
    List<Future<Boolean>> futures = new ArrayList<>(items);
    for (long id : ids) {
      Future<Boolean> future = executorService.submit(() -> filter.put(id));
      futures.add(future);
    }

    for (Future<Boolean> future : futures) {
      assertTrue(future.get());
    }
    futures.clear();
    assertEquals(items, filter.getItems());
    assertEquals((double) filter.getItems() / (filter.getCapacity()), filter.getLoadFactor(),
        0.001);

    // might contain
    for (long id : ids) {
      Future<Boolean> future = executorService.submit(() -> filter.mightContain(id));
      futures.add(future);
    }

    for (Future<Boolean> future : futures) {
      assertTrue(future.get());
    }
    futures.clear();

    // remove
    for (long id : ids) {
      Future<Boolean> future = executorService.submit(() -> filter.remove(id));
      futures.add(future);
    }

    for (Future<Boolean> future : futures) {
      assertTrue(future.get());
    }
    futures.clear();
    assertEquals(0, filter.getItems());
  }

}