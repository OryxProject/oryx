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

package com.cloudera.oryx.example;

import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.serving.ServingModel;
import com.cloudera.oryx.lambda.serving.ServingModelManager;

import java.io.IOException;
import java.util.Iterator;

public final class ExampleServingModelManager implements ServingModelManager<String> {

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) throws IOException {

  }

  @Override
  public ServingModel getModel() {
    return null;
  }

  @Override
  public void close() {

  }

}