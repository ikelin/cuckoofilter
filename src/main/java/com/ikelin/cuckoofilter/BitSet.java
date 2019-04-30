package com.ikelin.cuckoofilter;

import java.io.Serializable;

/**
 * A fixed length bit set that provides convenient methods for cuckoo table operations.
 */
public class BitSet implements Serializable {

  public static final long serialVersionUID = 7286845561221016229L;

  private static final long WORD_MASK = 0xffffffffffffffffL;
  private static final int ADDRESS_BITS_PER_WORD = 6;

  private final long size;
  private volatile long[] words;

  /**
   * Creates a bit set of the specified {@code size} with all bits initially set to {@code false}.
   *
   * @param size size of the bit set
   * @throws NegativeArraySizeException if {@code size} is a negative number
   */
  public BitSet(final long size) {
    if (size < 0) {
      throw new NegativeArraySizeException("size < 0: " + size);
    }

    this.size = size;
    this.words = new long[getWord(size - 1) + 1];
  }

  /**
   * Returns a {@code long} composed of bits from this bit set ranging from {@code fromIndex}
   * (inclusive) to {@code toIndex} (exclusive).
   *
   * @param fromIndex index of the first bit to include
   * @param toIndex index after the last bit to include
   * @return value of bits ranging from {@code fromIndex} to {@code toIndex}
   * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or {@code toIndex} is
   *     negative, or {@code fromIndex} is larger than {@code toIndex}
   * @throws IllegalArgumentException if {@code fromIndex} and {@code toIndex} are the same, or
   *     if the range from {@code fromIndex} to {@code toIndex} is greater than {@link
   *     java.lang.Integer#SIZE}.
   */
  public long get(final long fromIndex, final long toIndex) {
    checkRange(fromIndex, toIndex);

    int startWord = getWord(fromIndex);
    int endWord = getWord(toIndex - 1);

    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask = WORD_MASK >>> -toIndex;

    long value = 0;
    if (startWord == endWord) {
      value |= (words[startWord] & firstWordMask & lastWordMask) >>> fromIndex;
    } else {
      value |= (words[startWord] & firstWordMask) >>> fromIndex;
      value |= (words[endWord] & lastWordMask) << -fromIndex;
    }
    return value;
  }

  /**
   * Performs a logical OR on this bit set ranging from {@code fromIndex} (inclusive) to {@code
   * toIndex} (exclusive) with the bits from the provided {@code long}.
   *
   * @param fromIndex index of the first bit to perform logical OR
   * @param toIndex index after the last bit to perform logical OR
   * @param value value of bits to perform logical OR with
   * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or {@code toIndex} is
   *     negative, or {@code fromIndex} is larger than {@code toIndex}
   * @throws IllegalArgumentException if {@code fromIndex} and {@code toIndex} are the same, or
   *     if the range * from {@code fromIndex} to {@code toIndex} is greater than {@link
   *     java.lang.Long#SIZE}.
   */
  public void or(final long fromIndex, final long toIndex, final long value) {
    checkRange(fromIndex, toIndex);

    int startWord = getWord(fromIndex);
    int endWord = getWord(toIndex - 1);

    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask = WORD_MASK >>> -toIndex;

    if (startWord == endWord) {
      words[startWord] |= value << fromIndex & firstWordMask & lastWordMask;
    } else {
      words[startWord] |= value << fromIndex & firstWordMask;
      words[endWord] |= value >>> -fromIndex & lastWordMask;
    }
  }

  /**
   * Clears this bit set ranging from {@code fromIndex} (inclusive) to {@code toIndex} (exclusive)
   * by setting the bits to {@code false}.
   *
   * @param fromIndex index of the first bit to be cleared
   * @param toIndex index after the last bit to be cleared
   * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or {@code toIndex} is
   *     negative, or {@code fromIndex} is larger than {@code toIndex}
   * @throws IllegalArgumentException if range from {@code fromIndex} to {@code toIndex} is
   *     greater than {@link java.lang.Long#SIZE}.
   */
  public void clear(final long fromIndex, final long toIndex) {
    checkRange(fromIndex, toIndex);

    int startWord = getWord(fromIndex);
    int endWord = getWord(toIndex - 1);

    long firstWordMask = WORD_MASK << fromIndex;
    long lastWordMask = WORD_MASK >>> -toIndex;

    if (startWord == endWord) {
      words[startWord] &= ~(firstWordMask & lastWordMask);
    } else {
      words[startWord] &= ~firstWordMask;
      words[endWord] &= ~lastWordMask;
    }
  }

  public void checkRange(final long fromIndex, final long toIndex) {
    if (fromIndex < 0 || fromIndex >= size) {
      throw new IndexOutOfBoundsException(
          "fromIndex is not between 0 and " + size + ": " + fromIndex);
    }

    if (toIndex < 0 || toIndex > size) {
      throw new IndexOutOfBoundsException("toIndex is not between 0 and " + size + ": " + toIndex);
    }

    if (fromIndex > toIndex) {
      throw new IndexOutOfBoundsException(
          "fromIndex: " + fromIndex + " is not less than toIndex: " + toIndex);
    }

    if (fromIndex == toIndex) {
      throw new IllegalArgumentException("fromIndex and toIndex cannot be the same");
    }

    if (toIndex - fromIndex > Long.SIZE) {
      throw new IllegalArgumentException(
          "fromIndex and toIndex is not within " + Long.SIZE + " size: " + (toIndex - fromIndex));
    }
  }

  private static int getWord(final long bitIndex) {
    return (int) (bitIndex >> ADDRESS_BITS_PER_WORD);
  }

}
