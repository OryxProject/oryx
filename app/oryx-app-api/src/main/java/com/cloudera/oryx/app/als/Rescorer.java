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
 * Implementations of this interface can "rescore" the recommender's score for a item that is a candidate for
 * recommendation. It can boost or demote an item given its ID, or filter it out entirely.
 */
public interface Rescorer {

  /**
   * @param id ID of item to rescore
   * @param originalScore original score from the recommender
   * @return new score; return {@link Double#NaN} to exclude the item from recommendation
   */
  double rescore(String id, double originalScore);

  /**
   * @param id of item to consider for filtering
   * @return true iff the item should be removed from consideration
   */
  boolean isFiltered(String id);

}
