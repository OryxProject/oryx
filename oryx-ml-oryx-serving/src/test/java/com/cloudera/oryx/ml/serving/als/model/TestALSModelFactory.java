/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.ml.serving.als.model;

public final class TestALSModelFactory {

  private TestALSModelFactory() {}

  public static ALSServingModel buildTestModel() {
    // TODO
    ALSServingModel model = new ALSServingModel(2);
    model.setItemVector("A", new float[] {1.0f, 2.0f});
    model.setItemVector("B", new float[] {2.0f, 1.5f});
    model.setItemVector("C", new float[] {2.5f, 1.5f});
    model.setUserVector("Z", new float[] {3.0f, 4.0f});
    model.setUserVector("Y", new float[] {5.0f, 6.0f});
    return model;
  }

}
