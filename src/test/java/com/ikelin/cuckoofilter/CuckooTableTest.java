package com.ikelin.cuckoofilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CuckooTableTest {

  @Test
  void testContains() {
    CuckooTable table = new CuckooTable(3, 1, 16, 1);

    long value = 0xf00fL;
    assertTrue(table.addIfEmpty(1, value));
    assertTrue(table.contains(1, value));
    assertFalse(table.contains(0, value));
    assertFalse(table.contains(2, value));
  }

  @Test
  void testAddIfEmptySuccess() {
    CuckooTable table = new CuckooTable(3, 2, 12, 1);

    long value = 0xfffL;
    assertFalse(table.contains(1, value));
    assertTrue(table.addIfEmpty(1, value));
    assertTrue(table.contains(1, value));

    assertFalse(table.contains(0, value));
    assertFalse(table.contains(2, value));
  }

  @Test
  void testAddIfEmptyFail() {
    int entriesPerBucket = 2;
    CuckooTable table = new CuckooTable(8, entriesPerBucket, 4, 2);

    // fill the getBucketHash with values
    int existingValue = 0xf;
    for (int i = 0; i < entriesPerBucket; i++) {
      assertTrue(table.addIfEmpty(7, existingValue));
    }
    assertTrue(table.contains(7, existingValue));
    assertEquals(entriesPerBucket, table.count(7, existingValue));

    // add should fail
    long value = 0x1L;
    assertFalse(table.addIfEmpty(7, value));
    assertFalse(table.contains(7, value));

    // verify existing value exists
    assertTrue(table.contains(7, existingValue));
    assertEquals(entriesPerBucket, table.count(7, existingValue));
  }

  @Test
  void testRemoveSuccess() {
    CuckooTable table = new CuckooTable(8, 2, 32, 4);

    long firstValue = 0xffff0000L;
    assertTrue(table.addIfEmpty(1, firstValue));
    assertTrue(table.contains(1, firstValue));

    long secondValue = 0xffffL;
    assertTrue(table.addIfEmpty(1, secondValue));
    assertTrue(table.contains(1, secondValue));

    assertTrue(table.remove(1, firstValue));
    assertFalse(table.contains(1, firstValue));
    assertTrue(table.contains(1, secondValue));
  }

  @Test
  void testRemoveFail() {
    CuckooTable table = new CuckooTable(8, 2, 16, 4);

    long firstValue = 0xff00L;
    assertTrue(table.addIfEmpty(1, firstValue));
    assertTrue(table.contains(1, firstValue));

    long secondValue = 0xf00fL;
    assertFalse(table.remove(1, secondValue));
  }

  @Test
  void testGetAndSet() {
    CuckooTable table = new CuckooTable(8, 1, 11, 4);

    long firstValue = 1L << 10;
    assertTrue(table.addIfEmpty(1, firstValue));
    assertTrue(table.contains(1, firstValue));

    long secondValue = 1L << 9;
    assertEquals(firstValue, table.getAndSet(1, 0, secondValue));
    assertFalse(table.contains(1, firstValue));
    assertTrue(table.contains(1, secondValue));
  }

  @Test
  void testNegativeBuckets() {
    assertThrows(IllegalArgumentException.class, () -> new CuckooTable(0, 1, 1, 1));
  }

  @Test
  void testNegativeEntriesPerBucket() {
    assertThrows(IllegalArgumentException.class, () -> new CuckooTable(1, 0, 1, 1));
  }

  @Test
  void testNegativeBitsPerEntries() {
    assertThrows(IllegalArgumentException.class, () -> new CuckooTable(1, 1, 0, 1));
  }

  @Test
  void testNegativeConcurrentLevel() {
    assertThrows(IllegalArgumentException.class, () -> new CuckooTable(1, 1, 1, 0));
  }

}