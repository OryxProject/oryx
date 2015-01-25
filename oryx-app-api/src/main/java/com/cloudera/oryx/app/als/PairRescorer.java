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

package com.cloudera.oryx.app.als;

/**
 * Like {@link Rescorer}, but operates on pairs of item IDs, not single item IDs.
 */
public interface PairRescorer {

  /**
   * @param fromID ID of first item in pair to rescore
   * @param toID ID of second item in pair to rescore, usually the 'candidate' item
   *  under consideration
   * @param originalScore original score from the recommender
   * @return new score; return {@link Double#NaN} to exclude the pair from recommendation
   */
  double rescore(String fromID, String toID, double originalScore);

  /**
   * @param fromID ID of first item in pair to rescore
   * @param toID ID of second item in pair to rescore, usually the 'candidate' item
   *  under consideration
   * @return true iff the pair should be removed from consideration
   */
  boolean isFiltered(String fromID, String toID);

}
