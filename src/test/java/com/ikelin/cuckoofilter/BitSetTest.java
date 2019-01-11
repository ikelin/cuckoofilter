package com.ikelin.cuckoofilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BitSetTest {

  @Test
  void testInvalidBits() {
    assertThrows(NegativeArraySizeException.class, () -> new BitSet(-1L));
  }

  @Test
  void testGetBitInvalidBitIndex() {
    BitSet bitSet = new BitSet(4);
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.get(-1, 3));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.get(0, 5));
    assertThrows(IllegalArgumentException.class, () -> bitSet.get(0, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.get(4, 0));
  }

  @Test
  void testSetBitInvalidBitIndex() {
    BitSet bitSet = new BitSet(8);
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.or(-1, 7, 0xffL));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.or(0, 9, 0xffL));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.or(4, 0, 0xffL));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.or(0, 65, 0xffL));
  }

  @Test
  void testClearBitInvalidBitIndex() {
    BitSet bitSet = new BitSet(16);
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.clear(-1, 15));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.clear(0, 17));
    assertThrows(IndexOutOfBoundsException.class, () -> bitSet.clear(16, 0));
  }

  @Test
  void testOrBitsRangeOneWord() {
    BitSet bitSet = new BitSet(64);
    bitSet.or(0, 8, 0xffL);
    bitSet.or(28, 36, 0xffL);
    bitSet.or(56, 64, 0xffL);

    assertEquals(0xffL, bitSet.get(0, 8));
    assertEquals(0L, bitSet.get(8, 28));
    assertEquals(0xffL, bitSet.get(28, 36));
    assertEquals(0L, bitSet.get(36, 56));
    assertEquals(0xffL, bitSet.get(56, 64));

    bitSet.or(4, 12, 0xffL);
    assertEquals(0xfffL, bitSet.get(0, 12));
  }

  @Test
  void testOrBitsRangeMultiWords() {
    BitSet bitSet = new BitSet(128);
    bitSet.or(60, 68, 0xffL);
    assertEquals(0L, bitSet.get(0, 60));
    assertEquals(0xffL, bitSet.get(60, 68));
    assertEquals(0L, bitSet.get(68, 128));
  }

  @Test
  void testGetBitsRangeOneWord() {
    BitSet bitSet = new BitSet(64);
    bitSet.or(8, 16, 0xffL);
    assertEquals(0xffL, bitSet.get(8, 16));
    assertEquals(0xfL, bitSet.get(8, 12));
    assertEquals(0xfL, bitSet.get(12, 16));
    assertEquals(0xf0L, bitSet.get(4, 12));
    assertEquals(0xff00L, bitSet.get(0, 16));
  }

  @Test
  void testGetBitsRangeMultiWords() {
    BitSet bitSet = new BitSet(128);
    bitSet.or(56, 72, 0xffffL);
    assertEquals(0xffffL, bitSet.get(56, 72));
  }

  @Test
  void testClearBitsRangeOneWord() {
    BitSet bitSet = new BitSet(64);
    bitSet.or(0, 8, 0xffL);
    assertEquals(0xffL, bitSet.get(0, 8));
    bitSet.clear(0, 8);
    assertEquals(0L, bitSet.get(0, 8));

    bitSet.or(28, 36, 0xffL);
    assertEquals(0xffL, bitSet.get(28, 36));
    bitSet.clear(28, 36);
    assertEquals(0L, bitSet.get(28, 36));

    bitSet.or(56, 64, 0xffL);
    assertEquals(0xffL, bitSet.get(56, 64));
    bitSet.clear(56, 64);
    assertEquals(0L, bitSet.get(56, 64));
  }

  @Test
  void testClearBitsRangeMultiWords() {
    BitSet bitSet = new BitSet(128);
    bitSet.or(56, 64, 0xffL);
    assertEquals(0xffL, bitSet.get(56, 64));
    bitSet.clear(56, 64);
    assertEquals(0L, bitSet.get(56, 64));

    bitSet.or(60, 68, 0xffL);
    assertEquals(0xffL, bitSet.get(60, 68));
    bitSet.clear(60, 68);
    assertEquals(0L, bitSet.get(60, 68));

    bitSet.or(64, 72, 0xffL);
    assertEquals(0xffL, bitSet.get(64, 72));
    bitSet.clear(64, 72);
    assertEquals(0L, bitSet.get(64, 72));
  }
}