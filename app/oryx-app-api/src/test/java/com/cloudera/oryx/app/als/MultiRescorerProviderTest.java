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

import java.util.Collections;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link MultiRescorerProvider} (implementations).
 */
public final class MultiRescorerProviderTest extends OryxTest {
  
  @Test
  public void testMultiRecommendRescorer() {
    RescorerProvider multi = new MultiRescorerProvider(
        new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    
    Rescorer provider = multi.getRecommendRescorer(Collections.singletonList("ABCDE"), null);
    assertNull(provider);

    Rescorer provider2 = multi.getRecommendRescorer(Collections.singletonList("AB"), null);
    assertNotNull(provider2);
    assertFalse(provider2 instanceof MultiRescorer);
    assertTrue(provider2.isFiltered("ABC"));
    assertFalse(provider2.isFiltered("AB"));

    Rescorer provider3 = multi.getRecommendRescorer(Collections.singletonList("ABCDEF"), null);
    assertNotNull(provider3);
    assertTrue(provider3 instanceof MultiRescorer);
    assertTrue(provider3.isFiltered("ABC"));
    assertTrue(provider3.isFiltered("AB"));
    assertFalse(provider3.isFiltered("ABCDEFABCDEF"));
  }
  
  @Test
  public void testMultiRecommendToAnonymousRescorer() {
    RescorerProvider multi = new MultiRescorerProvider(
        new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    
    Rescorer provider = multi.getRecommendToAnonymousRescorer(
        Collections.singletonList("ABCDE"), null);
    assertNull(provider);

    Rescorer provider2 =
        multi.getRecommendToAnonymousRescorer(Collections.singletonList("AB"), null);
    assertNotNull(provider2);
    assertFalse(provider2 instanceof MultiRescorer);
    assertTrue(provider2.isFiltered("ABC"));
    assertFalse(provider2.isFiltered("AB"));

    Rescorer provider3 =
        multi.getRecommendToAnonymousRescorer(Collections.singletonList("ABCDEF"), null);
    assertNotNull(provider3);
    assertTrue(provider3 instanceof MultiRescorer);
    assertTrue(provider3.isFiltered("ABC"));
    assertTrue(provider3.isFiltered("AB"));
    assertFalse(provider3.isFiltered("ABCDEF"));
  }
  
  @Test
  public void testMultiMostPopularItemsRescorer() {
    RescorerProvider multi = new MultiRescorerProvider(
        new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    Rescorer provider = multi.getMostPopularItemsRescorer(null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertTrue(provider.isFiltered("AB"));
    assertFalse(provider.isFiltered("ABCDEF"));
  }

  @Test
  public void testMultiMostActiveUsersRescorer() {
    RescorerProvider multi = new MultiRescorerProvider(
        new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    Rescorer provider = multi.getMostActiveUsersRescorer(null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertTrue(provider.isFiltered("AB"));
    assertFalse(provider.isFiltered("ABCDEF"));
  }
  
  @Test
  public void testMultiMostSimilarItemsRescorer() {
    RescorerProvider multi = new MultiRescorerProvider(
        new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    Rescorer provider = multi.getMostSimilarItemsRescorer(null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertTrue(provider.isFiltered("ABCDE"));
    assertFalse(provider.isFiltered("ABCDEFABCDEF"));
  }
  
}
