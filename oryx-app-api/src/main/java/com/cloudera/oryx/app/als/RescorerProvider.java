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
 * <p>Implementations of this interface provide, optionally, objects that can be used to
 * modify and influence the results of endpoints like {@code Recommend}, {@code Similarity},
 * etc. It is a means to inject business logic into the results of the ALS application
 * endpoints.</p>
 *
 * <p>Implementations of this class are factories. An implementation creates and configures
 * a {@link Rescorer} rescoring object and returns it for use in the context of one
 * of the endpoints' operation. For example, the result of
 * {@link #getRecommendRescorer(List, List)} is used with {@code Recommend}.
 * The {@link Rescorer} filters the candidates recommendations by item ID
 * ({@link Rescorer#isFiltered(String)}) or modifies the scores of item candidates that
 * are not filtered ({@link Rescorer#rescore(String, double)}) based on the item ID and
 * original score.</p>
 *
 * <p>The factory methods take several optional {@code String} arguments. These are passed
 * from the REST API, as several {@code String}s, from URL parameter {@code rescorerParams}.
 * The implementation may need this information to initialize its rescoring logic for the request.
 * For example, the argument may be the user's current location, used to filter
 * results by location.</p>
 *
 * <p>For example, a request containing {@code ...?rescorerParams=xxx,yyy,zzz} will result in an
 * {@code args} parameter with <em>one</em> elements, {@code xxx,yyy,zzz}. A request containing
 * {@code ...?rescorerParams=xxx&...rescorerParams=yyy&...rescorerParams=zzz...} will result in an
 * {@code args} parameter with 3 elements, {@code xxx}, {@code yyy}, {@code zzz}.</p>
 */
public interface RescorerProvider {

  /**
   * @param userIDs user(s) for which recommendations are being made, which may be needed
   *  in the rescoring logic.
   * @param args arguments, if any, that should be used when making the {@link Rescorer}.
   *  This is additional information from the request that may be necessary to its logic,
   *  like current location. What it means is up to the implementation.
   * @return {@link Rescorer} to use or {@code null} if none should be used.
   *  The resulting {@link Rescorer} will be passed each candidate item ID to
   *  {@link Rescorer#isFiltered(String)}, and each non-filtered candidate with its original score
   *  to {@link Rescorer#rescore(String, double)}
   */
  Rescorer getRecommendRescorer(List<String> userIDs, List<String> args);

  /**
   * @param itemIDs items that the anonymous user is associated to
   * @param args arguments, if any, that should be used when making the {@link Rescorer}.
   *  This is additional information from the request that may be necessary to its logic,
   *  like current location. What it means is up to the implementation.
   * @return {@link Rescorer} to use or {@code null} if none should be used.
   *  The resulting {@link Rescorer} will be passed each candidate item ID to
   *  {@link Rescorer#isFiltered(String)}, and each non-filtered candidate with its original score
   *  to {@link Rescorer#rescore(String, double)}
   */
  Rescorer getRecommendToAnonymousRescorer(List<String> itemIDs, List<String> args);

  /**
   * @param args arguments, if any, that should be used when making the {@link Rescorer}.
   *  This is additional information from the request that may be necessary to its logic,
   *  like current location. What it means is up to the implementation.
   * @return {@link Rescorer} to use or {@code null} if none should be used.
   */
  Rescorer getMostPopularItemsRescorer(List<String> args);

  /**
   * @param args arguments, if any, that should be used when making the {@link Rescorer}.
   *  This is additional information from the request that may be necessary to its logic,
   *  like current location. What it means is up to the implementation.
   * @return {@link PairRescorer} to use or {@code null} if none should be used.
   *  The {@link PairRescorer} will be passed, to its
   *  {@link PairRescorer#isFiltered(String, String)} method, the candidate item ID
   *  and item ID passed in the user query as its second element.
   *  Each non-filtered pair is passed with its original score to
   *  {@link PairRescorer#rescore(String, String, double)}
   */
  PairRescorer getMostSimilarItemsRescorer(List<String> args);

}
