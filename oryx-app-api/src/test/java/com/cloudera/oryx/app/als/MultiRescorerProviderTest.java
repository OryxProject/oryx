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

/**
 * Tests {@link MultiRescorerProvider} (implementations).
 */
public final class MultiRescorerProviderTest extends OryxTest {
  
  @Test
  public void testMultiRecommendRescorer() {
    RescorerProvider multi =
        new MultiRescorerProvider(new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    
    Rescorer provider = multi.getRecommendRescorer(new String[]{"ABCDE"}, null);
    assertNull(provider);
    
    provider = multi.getRecommendRescorer(new String[]{"AB"}, null);
    assertNotNull(provider);
    assertFalse(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertFalse(provider.isFiltered("AB"));

    provider = multi.getRecommendRescorer(new String[]{"ABCDEF"}, null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertTrue(provider.isFiltered("AB"));
    assertFalse(provider.isFiltered("ABCDEFABCDEF"));
  }
  
  @Test
  public void testMultiRecommendToAnonymousRescorer() {
    RescorerProvider multi = 
        new MultiRescorerProvider(new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    
    Rescorer provider = multi.getRecommendToAnonymousRescorer(new String[]{"ABCDE"}, null);
    assertNull(provider);
    
    provider = multi.getRecommendToAnonymousRescorer(new String[]{"AB"}, null);
    assertNotNull(provider);
    assertFalse(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertFalse(provider.isFiltered("AB"));

    provider = multi.getRecommendToAnonymousRescorer(new String[]{"ABCDEF"}, null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertTrue(provider.isFiltered("AB"));
    assertFalse(provider.isFiltered("ABCDEF"));
  }
  
  @Test
  public void testMultiMostPopularItemsRescorer() {
    RescorerProvider multi =
        new MultiRescorerProvider(new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    Rescorer provider = multi.getMostPopularItemsRescorer(null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiRescorer);
    assertTrue(provider.isFiltered("ABC"));
    assertTrue(provider.isFiltered("AB"));
    assertFalse(provider.isFiltered("ABCDEF"));
  }
  
  @Test
  public void testMultiMostSimilarItemsRescorer() {
    RescorerProvider multi = 
        new MultiRescorerProvider(new SimpleModRescorerProvider(2), new SimpleModRescorerProvider(3));
    PairRescorer provider = multi.getMostSimilarItemsRescorer(null);
    assertNotNull(provider);
    assertTrue(provider instanceof MultiPairRescorer);
    assertTrue(provider.isFiltered("AB", "ABC"));
    assertTrue(provider.isFiltered("AB", "ABCDEF"));
    assertFalse(provider.isFiltered("ABCDEF", "ABCDEFABCDEF"));
  }
  
}
