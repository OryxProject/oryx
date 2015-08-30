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

package com.cloudera.oryx.lambda.serving;

import java.util.Iterator;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.serving.ServingModelManager;

public final class MockServingModelManager implements ServingModelManager<String> {

  @Override
  public void consume(Iterator<KeyMessage<String, String>> updateIterator, Configuration hadoopConf) {
    // do nothing
  }

  @Override
  public Config getConfig() {
    return null;
  }

  @Override
  public Object getModel() {
    return null;
  }

  @Override
  public void close() {
    // do nothing
  }

}
