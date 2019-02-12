package com.ikelin.cuckoofilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CuckooFilterTest {

  private static long fooHash;
  private static long barHash;

  @BeforeAll
  static void beforeAll() {
    fooHash = 7519298183266517360L;
    barHash = -377454365428918003L;
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
    CuckooFilter filter = CuckooFilter.create(100).withFalsePositiveProbability(0.01D).build();
    assertEquals(64, filter.getBuckets());
    assertEquals(2, filter.getEntriesPerBucket());
    assertEquals(12, filter.getBitsPerEntry());

    filter = CuckooFilter.create(100).withFalsePositiveProbability(0.001D).build();
    assertEquals(32, filter.getBuckets());
    assertEquals(4, filter.getEntriesPerBucket());
    assertEquals(14, filter.getBitsPerEntry());

    filter = CuckooFilter.create(100).withFalsePositiveProbability(0.000001D).build();
    assertEquals(16, filter.getBuckets());
    assertEquals(8, filter.getEntriesPerBucket());
    assertEquals(24, filter.getBitsPerEntry());
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

    int availableProcessors = Runtime.getRuntime().availableProcessors();
    filter = CuckooFilter.create(100).build();
    if (availableProcessors > filter.getBuckets()) {
      assertEquals(filter.getBuckets(), filter.getConcurrencyLevel());
    } else {
      assertEquals(availableProcessors, filter.getConcurrencyLevel());
    }
  }

  @Test
  void testCreateInvalid() {
    assertThrows(IllegalArgumentException.class, () -> CuckooFilter.create(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withFalsePositiveProbability(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withFalsePositiveProbability(1).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withBitsPerEntry(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withBitsPerEntry(Integer.SIZE).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withEntriesPerBucket(0).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withEntriesPerBucket(5).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withEntriesPerBucket(16).build());
    assertThrows(IllegalArgumentException.class,
        () -> CuckooFilter.create(100).withConcurrencyLevel(0).build());
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

}