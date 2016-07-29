package org.apache.parquet.benchmarks;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

import java.io.IOException;
import java.lang.reflect.Field;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmark of various approaches for vectorized generation of offsets from repetition levels
 */
public class VectorizedConversionBenchmark {

  /**
   * turns repetition levels into offsets
   */
  public interface ComputeOffset {
    /**
     * there are as many offset vectors as the max repetition level
     *
     * @param rl the repetition levels
     * @param offset the offset vectors
     */
    void computeOffsets(int[] rl, int[][] offset);
  }

  /**
   * Baseline implementation
   * the offset vector gets incremented if it is higher than the repetition level
   * We will want to avoid the branches
   */
  @State(Benchmark)
  public static class COBaseLine implements ComputeOffset {
    public void computeOffsets(int[] rl, int[][] offset) {
      int[] currentIndex = new int[offset.length + 1]; // current index for each
      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        for (int j = 0; j < currentIndex.length; j++) {
          if (r <= j) { // branch!
            currentIndex[j] ++;
            if (j >= 1) { // branch!
              offset[j - 1][currentIndex[j - 1]] = currentIndex[j];
            }
          }
        }
      }
    }
  }

  /**
   * instead of branching, precompute an array with 0 or 1 whether we need to increment or not
   */
  @State(Benchmark)
  public static class COArrayInc implements ComputeOffset {
    // will work only for max rl 2
    // precompute the increment to avoid branching
    int[][] toAdd = computeIncArray(2);

    public void computeOffsets(int[] rl, int[][] offset) {
      int[] currentIndex = new int[offset.length + 1];
      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        for (int j = 0; j < currentIndex.length; j++) { //branch !
          currentIndex[j] += toAdd[j][r];
        }
        for (int j = 1; j < currentIndex.length; j++) { //branch !
          offset[j - 1][currentIndex[j - 1]] = currentIndex[j];
        }
      }
    }

    private int[][] computeIncArray(int offsetLength) {
      int[][] toAdd = new int[offsetLength + 1][];
//      System.out.print("i\\j");
//      for (int j = 0; j < offset.length + 1; j++) {
//        System.out.print(j + " | ");
//      }
//      System.out.println();
      for (int i = 0; i < toAdd.length; i++) {
        toAdd[i] = new int[offsetLength + 1];
      }
      for (int i = 0; i < toAdd.length; i++) {
//        System.out.print(i + ": ");
        for (int j = 0; j < toAdd[i].length; j++) {
          toAdd[j][i] = i <= j ? 1 : 0;
          //        System.out.print(toAdd[i][j] + " | ");
//          System.out.print(((j | (j >> 1)) &1) + " | ");
        }
//        System.out.println();
      }
      return toAdd;
    }
  }

  /**
   * COArrayInc but we pregenerate for max repetition level 2
   */
  @State(Benchmark)
  public static class COArrayInc2 extends COArrayInc {

    public void computeOffsets(int[] rl, int[][] offset) {
      int currentIndex0 = 0;
      int currentIndex1 = 0;
      int currentIndex2 = 0;
      int[] toAdd0 = toAdd[0];
      int[] toAdd1 = toAdd[1];
      int[] offset0 = offset[0];
      int[] offset1 = offset[1];
      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        currentIndex0 += toAdd0[r];
        currentIndex1 += toAdd1[r];
        currentIndex2 += 1; // always increment
        offset0[currentIndex0] = currentIndex1;
        offset1[currentIndex1] = currentIndex2;
      }
    }

  }

  /**
   * Produce a binary formula instead of an array lookup
   */
  @State(Benchmark)
  public static class COBinaryInc implements ComputeOffset {
    public  void computeOffsets(int[] rl, int[][] offset) {
      int currentIndex0 = 0;
      int currentIndex1 = 0;
      int currentIndex2 = 0;
      int[] o0 = offset[0];
      int[] o1 = offset[1];

      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        // instead of the array lookup approach, we figure out a formula
        currentIndex0 += (((r & 1) | (r >>> 1)) ^ 1); // inc if r == 0 only
        currentIndex1 += ((r >>> 1) ^ 1); // inc if r == 0 or 1
        currentIndex2 += 1; // inc if r == 0, 1 or 2

        // TODO: check if we can remove bound checks here
        o0[currentIndex0] = currentIndex1;
        o1[currentIndex1] = currentIndex2;
      }
    }
  }

  /**
   * another approach to COBinaryInc
   */
  @State(Benchmark)
  public static class COBinaryInc2 implements ComputeOffset {

    public  void computeOffsets(int[] rl, int[][] offset) {
      int currentIndex0 = 0;
      int currentIndex1 = 0;
      int currentIndex2 = 0;
      int[] o0 = offset[0];
      int[] o1 = offset[1];

      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        // instead of the array lookup approach, we figure out a formula
        currentIndex0 += (1 >>> r) & 1; // inc if r == 0 only
        currentIndex1 += (3 >>> r) & 1; // inc if r == 0 or 1
        currentIndex2 += 1; // inc if r == 0, 1 or 2. Which is always since max rl = 2

        // TODO: check if we can remove bound checks here
        o0[currentIndex0] = currentIndex1;
        o1[currentIndex1] = currentIndex2;
      }
    }
  }

  /**
   * See if using Unsafe makes array access faster
   */
  @State(Benchmark)
  public static class COBinaryIncU implements ComputeOffset {
    sun.misc.Unsafe unsafe;
    {
      try {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        unsafe = (sun.misc.Unsafe) f.get(null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    int arrayBaseOffset = unsafe.arrayBaseOffset(int[].class);

    public  void computeOffsets(int[] rl, int[][] offset) {
      int currentIndex0 = 0;
      int currentIndex1 = 0;
      int currentIndex2 = 0;
      int[] o0 = offset[0];
      int[] o1 = offset[1];

      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        currentIndex0 += (((r & 1) | (r >>> 1)) ^ 1); // inc if r == 0 only
        currentIndex1 += ((r >>> 1) ^ 1); // inc if r == 0 or 1
        currentIndex2 += 1; // inc if r == 0, 1 or 2

        unsafe.putInt(o0, arrayBaseOffset + currentIndex0 * 4, currentIndex1);
        unsafe.putInt(o1, arrayBaseOffset + currentIndex1 * 4, currentIndex2);
      }
    }
  }

  /**
   * Just checking if ternary operator
   */
  @State(Benchmark)
  public static class COTernaryInc implements ComputeOffset {
    public void computeOffsets(int[] rl, int[][] offset) {
      int currentIndex0 = 0;
      int currentIndex1 = 0;
      int currentIndex2 = 0;
      int[] o0 = offset[0];
      int[] o1 = offset[1];

      for (int i = 0; i < rl.length; i++) {
        int r = rl[i]; // clearly iterating on array length, so should not bound check
        currentIndex0 += r == 0 ? 1 : 0;
        currentIndex1 += r <= 1 ? 1 : 0;
        currentIndex2 += 1;

        o0[currentIndex0] = currentIndex1;
        o1[currentIndex1] = currentIndex2;
      }
    }
  }

  static int FACTOR = 10000;
  // input data
  static int[] EXAMPLE_RL = { 0, 2, 2, 1, 2, 2, 2, 0, 1, 2};
  // output offset vector
  static int[][] EXAMPLE_OFFSET = { { 0, 0, 0 }, { 0, 0, 0, 0, 0 } };
  static int[][] OFFSET = growArrays(EXAMPLE_OFFSET, FACTOR);
  static int[] RL = growArray(EXAMPLE_RL, FACTOR);

  static int[] growArray(int[] array, int factor) {
    int[] newArray = new int[array.length * factor];
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < factor; j++) {
        newArray[j * array.length + i] = array[i];
      }
    }
    return newArray;
  }

  static int[][] growArrays(int[][] array, int factor) {
    int[][] newArray = new int[array.length][];
    for (int i = 0; i < array.length; i++) {
      newArray[i] = growArray(array[i], factor);
    }
    return newArray;
  }

  @Benchmark
  public void baseLine(COBaseLine c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

  @Benchmark
  public void arrayInc(COArrayInc c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

  @Benchmark
  public void arrayInc2(COArrayInc2 c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

  @Benchmark
  public void binaryInc(COBinaryInc c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

  @Benchmark
  public void binaryInc2(COBinaryInc2 c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

  @Benchmark
  public void binaryIncU(COBinaryIncU c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

  @Benchmark
  public void ternaryInc(COTernaryInc c) throws IOException {
    c.computeOffsets(RL, OFFSET);
  }

}
