/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.app.serving.als.model;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.random.RandomManager;

final class LocalitySensitiveHash {

  static final int MAX_HASHES = 16;
  private static final Logger log = LoggerFactory.getLogger(LocalitySensitiveHash.class);

  private final float[][] hashVectors;
  private final int maxBitsDiffering;
  private final int[] candidateIndicesPrototype;
  private final int[] allIndices;

  LocalitySensitiveHash(double sampleRate, int numFeatures) {
    this(sampleRate, numFeatures, Runtime.getRuntime().availableProcessors());
  }

  // This separate constructor exists for unit testing only
  LocalitySensitiveHash(double sampleRate, int numFeatures, int numCores) {

    // How many hash functions to use? use as few as possible that still achieve the desired sample
    // rate or less, approximately.
    int numHashes = 0;
    int bitsDiffering = 0;
    for (; numHashes < MAX_HASHES; numHashes++) {

      // For a given number of hashes, consider partitions differing from the target hash in how many bits?
      // Choose enough such that number to test is as large as possible while <= the number of cores
      bitsDiffering = 0;
      // Number of different partitions that are examined when allowing the given number of bits to differ
      long numPartitionsToTry = 1;
      // Make bitsDiffering as large as possible given number of cores
      while (bitsDiffering < numHashes && numPartitionsToTry < numCores) {
        // There are numHashes-choose-bitsDiffering ways for numHashes bits to differ in
        // exactly bitsDiffering bits
        bitsDiffering++;
        numPartitionsToTry += CombinatoricsUtils.binomialCoefficient(numHashes, bitsDiffering);
      }
      // Note that this allows numPartitionsToTry to overshoot numCores by one step

      if (bitsDiffering == numHashes && numPartitionsToTry < numCores) {
        // Can't keep busy enough; keep going
        continue;
      }

      // Consider what fraction of all 2^n partitions is then considered, as a proxy for the
      // sample rate
      // Stop as soon as it's <= target sample rate
      if (numPartitionsToTry <= sampleRate * (1L << numHashes)) {
        break;
      }
    }

    log.info("LSH with {} hashes, querying partitions with up to {} bits differing", numHashes, bitsDiffering);
    this.maxBitsDiffering = bitsDiffering;
    hashVectors = new float[numHashes][];

    RandomGenerator random = RandomManager.getRandom();
    for (int i = 0; i < numHashes; i++) {
      // Pick the most-orthogonal next random vector
      double bestTotalDot = Double.POSITIVE_INFINITY;
      float[] nextBest = null;
      // Try, like, lots of them
      int candidatesSinceBest = 0;
      while (candidatesSinceBest < 1000) {
        float[] candidate = VectorMath.randomVectorF(numFeatures, random);
        // measure by total (absolute) dot product
        double score = totalAbsCos(hashVectors, i, candidate);
        if (score < bestTotalDot) {
          nextBest = candidate;
          // Stop if best possible score
          if (score == 0.0) {
            break;
          }
          bestTotalDot = score;
          candidatesSinceBest = 0;
        } else {
          candidatesSinceBest++;
        }
      }
      hashVectors[i] = nextBest;
    }
    log.info("Chose {} random hash vectors", hashVectors.length);

    // Contains all 2^numHashes integers from 0. The first element has 0 bits set. The next numHashes elements
    // are all such integers with 1 bit sets. Then 2 bits, and so on. This is used as a "mask" on top of an
    // initial candidate index in order to construct results in getCandidateIndices()
    candidateIndicesPrototype = new int[1 << numHashes];
    int[] offsetPerBitsActive = new int[numHashes + 1];
    for (int i = 1; i <= numHashes; i++) {
      offsetPerBitsActive[i] = offsetPerBitsActive[i-1] + (int) CombinatoricsUtils.binomialCoefficient(numHashes, i-1);
    }
    for (int i = 0; i < candidateIndicesPrototype.length; i++) {
      candidateIndicesPrototype[offsetPerBitsActive[Integer.bitCount(i)]++] = i;
    }

    // Contains all 2^numHashes integers from 0
    allIndices = new int[1 << numHashes];
    for (int i = 0; i < allIndices.length; i++) {
      allIndices[i] = i;
    }
  }

  int getNumHashes() {
    return hashVectors.length;
  }

  int getNumPartitions() {
    return 1 << getNumHashes();
  }

  int getMaxBitsDiffering() {
    return maxBitsDiffering;
  }

  /**
   * @param vector vector to hash
   * @return index of partition into which it hashes
   */
  int getIndexFor(float[] vector) {
    int index = 0;
    for (int i = 0; i < hashVectors.length; i++) {
      if (VectorMath.dot(hashVectors[i], vector) > 0.0) {
        index |= 1 << i;
      }
    }
    return index;
  }

  /**
   * @param vector vector whose dot product with hashed vectors is to be maximized
   * @return indices of partitions containing candidates to check
   */
  int[] getCandidateIndices(float[] vector) {
    int mainIndex = getIndexFor(vector);
    // Simple cases
    int numHashes = getNumHashes();
    if (numHashes == maxBitsDiffering) {
      return allIndices;
    }
    if (maxBitsDiffering == 0) {
      return new int[] { mainIndex };
    }
    // Other cases
    int howMany = 0;
    for (int i = 0; i <= maxBitsDiffering; i++) {
      howMany += (int) CombinatoricsUtils.binomialCoefficient(numHashes, i);
    }
    int[] result = new int[howMany];
    System.arraycopy(candidateIndicesPrototype, 0, result, 0, howMany);
    for (int i = 0; i < howMany; i++) {
      result[i] ^= mainIndex;
    }
    return result;
  }

  private static double totalAbsCos(float[][] existingVectors, int numExisting, float[] newVector) {
    double newNorm = VectorMath.norm(newVector);
    double sum = 0.0;
    for (int i = 0; i < numExisting; i++) {
      sum += Math.abs(VectorMath.cosineSimilarity(existingVectors[i], newVector, newNorm));
    }
    return sum;
  }

}
