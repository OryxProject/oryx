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

import java.util.List;

/**
 * Abstract implementation of {@link RescorerProvider} which implements all methods to
 * return {@code null}.
 */
public abstract class AbstractRescorerProvider implements RescorerProvider {

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getRecommendRescorer(List<String> userIDs, List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getRecommendToAnonymousRescorer(List<String> itemIDs, List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getMostPopularItemsRescorer(List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getMostActiveUsersRescorer(List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getMostSimilarItemsRescorer(List<String> args) {
    return null;
  }

}
