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

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class AbstractRescorerProviderTest extends OryxTest {

  @Test
  public void testDefault() {
    RescorerProvider noop = new NullProvider1();
    assertNull(noop.getMostActiveUsersRescorer(null));
    assertNull(noop.getMostPopularItemsRescorer(null));
    assertNull(noop.getMostSimilarItemsRescorer(null));
    assertNull(noop.getRecommendRescorer(null, null));
    assertNull(noop.getRecommendToAnonymousRescorer(null, null));
  }

  @Test
  public void testLoad() {
    assertNull(AbstractRescorerProvider.loadRescorerProviders(null));
    RescorerProvider provider = AbstractRescorerProvider.loadRescorerProviders(
        "com.cloudera.oryx.app.als.NullProvider2");
    assertTrue(provider instanceof NullProvider2);
    RescorerProvider multiProvider = AbstractRescorerProvider.loadRescorerProviders(
        "com.cloudera.oryx.app.als.NullProvider1,com.cloudera.oryx.app.als.NullProvider2");
    assertTrue(multiProvider instanceof MultiRescorerProvider);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoClass() {
    AbstractRescorerProvider.loadRescorerProviders("noSuchClass");
  }

  @Test(expected = IllegalStateException.class)
  public void testBadInstantiation() {
    AbstractRescorerProvider.loadRescorerProviders("com.cloudera.oryx.app.als.ErrorProvider");
  }

  @Test(expected = ClassCastException.class)
  public void testWrongClass() {
    AbstractRescorerProvider.loadRescorerProviders(
        "com.cloudera.oryx.app.als.AbstractRescorerProviderTest");
  }

}
