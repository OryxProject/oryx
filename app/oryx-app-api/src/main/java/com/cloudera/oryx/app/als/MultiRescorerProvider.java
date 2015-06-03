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
import java.util.Collections;
import java.util.List;

/**
 * Convenience implementation that will aggregate the behavior of {@link RescorerProvider}s.
 * It will filter an item if any of the given instances filter it, and will rescore by applying
 * the rescorings in the given order.
 *
 * @see MultiRescorer
 */
public final class MultiRescorerProvider extends AbstractRescorerProvider {

  private final RescorerProvider[] providers;

  private MultiRescorerProvider(RescorerProvider... providers) {
    this.providers = providers;
  }

  public static RescorerProvider of(RescorerProvider... providers) {
    if (providers.length == 0) {
      throw new IllegalArgumentException("providers is empty");
    }
    List<RescorerProvider> expandedProviders = new ArrayList<>();
    for (RescorerProvider provider : providers) {
      // Assuming at most one level of nesting here
      if (provider instanceof MultiRescorerProvider) {
        Collections.addAll(expandedProviders, ((MultiRescorerProvider) provider).getProviders());
      } else {
        expandedProviders.add(provider);
      }
    }
    return new MultiRescorerProvider(expandedProviders.toArray(new RescorerProvider[expandedProviders.size()]));
  }

  RescorerProvider[] getProviders() {
    return providers;
  }

  @Override
  public Rescorer getRecommendRescorer(List<String> userIDs, List<String> args) {
    List<Rescorer> rescorers = new ArrayList<>(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getRecommendRescorer(userIDs, args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers);
  }

  @Override
  public Rescorer getRecommendToAnonymousRescorer(List<String> itemIDs, List<String> args) {
    List<Rescorer> rescorers = new ArrayList<>(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getRecommendToAnonymousRescorer(itemIDs, args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers);
  }

  @Override
  public Rescorer getMostPopularItemsRescorer(List<String> args) {
    List<Rescorer> rescorers = new ArrayList<>(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getMostPopularItemsRescorer(args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers);
  }

  @Override
  public Rescorer getMostActiveUsersRescorer(List<String> args) {
    List<Rescorer> rescorers = new ArrayList<>(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getMostActiveUsersRescorer(args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    return buildRescorer(rescorers);
  }

  private static Rescorer buildRescorer(List<Rescorer> rescorers) {
    int numRescorers = rescorers.size();
    if (numRescorers == 0) {
      return null;
    }
    if (numRescorers == 1) {
      return rescorers.get(0);
    }
    return MultiRescorer.of(rescorers);
  }

  @Override
  public Rescorer getMostSimilarItemsRescorer(List<String> args) {
    List<Rescorer> rescorers = new ArrayList<>(providers.length);
    for (RescorerProvider provider : providers) {
      Rescorer rescorer = provider.getMostSimilarItemsRescorer(args);
      if (rescorer != null) {
        rescorers.add(rescorer);
      }
    }
    int numRescorers = rescorers.size();
    if (numRescorers == 0) {
      return null;
    }
    if (numRescorers == 1) {
      return rescorers.get(0);
    }
    return MultiRescorer.of(rescorers);
  }

}
