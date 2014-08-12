/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.serving.ServingModelManager;

import java.io.IOException;
import java.util.Iterator;

public final class MockALSServingModelManager implements ServingModelManager<String> {

  private static MockALSServingModelManager INSTANCE = null;

  public static MockALSServingModelManager getInstance() {
    return INSTANCE;
  }

  private final ALSServingModelManager delegate;

  public MockALSServingModelManager() {
    delegate = new ALSServingModelManager();
    INSTANCE = this;
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) throws IOException {
    delegate.consume(updateIterator);
  }

  @Override
  public ALSServingModel getModel() {
    return delegate.getModel();
  }

  @Override
  public void close() {
    delegate.close();
  }

}
