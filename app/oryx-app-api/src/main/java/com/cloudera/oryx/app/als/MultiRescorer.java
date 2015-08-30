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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Convenience implementation that will aggregate the behavior of multiple {@link Rescorer}s.
 * It will filter an item if any of the given instances filter it, and will rescore by applying
 * the rescorings in the given order.
 *
 * @see MultiRescorerProvider
 */
public final class MultiRescorer implements Rescorer {

  private final Rescorer[] rescorers;

  private MultiRescorer(Rescorer... rescorers) {
    this.rescorers = rescorers;
  }

  /**
   * @param rescorers {@link Rescorer} objects to delegate to
   * @return {@link Rescorer} aggregating their behaviors
   */
  public static Rescorer of(Rescorer... rescorers) {
    return of(Arrays.asList(rescorers));
  }

  static Rescorer of(List<Rescorer> rescorers) {
    if (rescorers.isEmpty()) {
      throw new IllegalArgumentException("rescorers is empty");
    }
    List<Rescorer> expandedRescorers = new ArrayList<>();
    for (Rescorer rescorer : rescorers) {
      // Assuming at most one level of nesting here
      if (rescorer instanceof MultiRescorer) {
        Collections.addAll(expandedRescorers, ((MultiRescorer) rescorer).getRescorers());
      } else {
        expandedRescorers.add(rescorer);
      }
    }
    return new MultiRescorer(expandedRescorers.toArray(new Rescorer[expandedRescorers.size()]));
  }

  Rescorer[] getRescorers() {
    return rescorers;
  }

  @Override
  public double rescore(String itemID, double value) {
    for (Rescorer rescorer : rescorers) {
      value = rescorer.rescore(itemID, value);
      if (Double.isNaN(value)) {
        return Double.NaN;
      }
    }
    return value;
  }

  @Override
  public boolean isFiltered(String itemID) {
    for (Rescorer rescorer : rescorers) {
      if (rescorer.isFiltered(itemID)) {
        return true;
      }
    }
    return false;
  }

}
