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

package com.cloudera.oryx.app.serving.als;

import java.util.List;

import com.cloudera.oryx.app.als.Rescorer;
import com.cloudera.oryx.app.als.RescorerProvider;

public final class TestALSRescorerProvider implements RescorerProvider {

  private static final Rescorer TEST_RESCORER = new Rescorer() {
    @Override
    public double rescore(String id, double originalScore) {
      return isFiltered(id) ? Double.NaN : originalScore * 2.0;
    }
    @Override
    public boolean isFiltered(String id) {
      return id.charAt(id.length() - 1) % 2 == 0;
    }
  };

  private static Rescorer buildRescorer(List<String> args) {
    return args == null || args.isEmpty() ? null : TEST_RESCORER;
  }


  @Override
  public Rescorer getRecommendRescorer(List<String> userIDs, List<String> args) {
    return buildRescorer(args);
  }

  @Override
  public Rescorer getRecommendToAnonymousRescorer(List<String> itemIDs, List<String> args) {
    return buildRescorer(args);
  }

  @Override
  public Rescorer getMostPopularItemsRescorer(List<String> args) {
    return buildRescorer(args);
  }

  @Override
  public Rescorer getMostActiveUsersRescorer(List<String> args) {
    return buildRescorer(args);
  }

  @Override
  public Rescorer getMostSimilarItemsRescorer(List<String> args) {
    return buildRescorer(args);
  }

}
