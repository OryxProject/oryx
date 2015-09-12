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

package com.cloudera.oryx.app.serving.als.model;

import org.junit.Test;

import com.cloudera.oryx.app.als.MultiRescorerProvider;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.common.OryxTest;

public class ALSServingModelManagerTest extends OryxTest {

  @Test
  public void testLoad() {
    assertNull(ALSServingModelManager.loadRescorerProviders(null));
    RescorerProvider provider = ALSServingModelManager.loadRescorerProviders(
        "com.cloudera.oryx.app.serving.als.model.NullProvider2");
    assertTrue(provider instanceof NullProvider2);
    RescorerProvider multiProvider = ALSServingModelManager.loadRescorerProviders(
        "com.cloudera.oryx.app.serving.als.model.NullProvider2," +
        "com.cloudera.oryx.app.serving.als.model.NullProvider2");
    assertTrue(multiProvider instanceof MultiRescorerProvider);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoClass() {
    ALSServingModelManager.loadRescorerProviders("noSuchClass");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadInstantiation() {
    ALSServingModelManager.loadRescorerProviders("com.cloudera.oryx.app.als.ErrorProvider");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongClass() {
    ALSServingModelManager.loadRescorerProviders(
        "com.cloudera.oryx.app.als.AbstractRescorerProviderTest");
  }

}
