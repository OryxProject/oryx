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

final class SimpleModRescorerProvider extends AbstractRescorerProvider {
  
  private final int modulus;
  
  SimpleModRescorerProvider(int modulus) {
    this.modulus = modulus;
  }

  @Override
  public Rescorer getRecommendRescorer(List<String> userIDs, List<String> args) {
    return userIDs.get(0).length() % modulus == 0 ? new SimpleModRescorer(modulus) : null;
  }

  @Override
  public Rescorer getRecommendToAnonymousRescorer(List<String> itemIDs, List<String> args) {
    return itemIDs.get(0).length() % modulus == 0 ? new SimpleModRescorer(modulus) : null;
  }

  @Override
  public Rescorer getMostPopularItemsRescorer(List<String> args) {
    return new SimpleModRescorer(modulus);
  }

  @Override
  public PairRescorer getMostSimilarItemsRescorer(List<String> args) {
    return new SimpleModRescorer(modulus);
  }

}
