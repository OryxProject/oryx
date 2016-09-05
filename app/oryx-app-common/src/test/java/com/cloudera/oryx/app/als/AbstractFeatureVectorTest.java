/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;

import com.cloudera.oryx.common.OryxTest;

abstract class AbstractFeatureVectorTest extends OryxTest {

  private ExecutorService executor = null;

  synchronized ExecutorService getExecutor() {
    if (executor == null) {
      executor = Executors.newCachedThreadPool();
    }
    return executor;
  }

  @After
  public synchronized void tearDownExecutor() {
    if (executor != null) {
      executor.shutdown();
      executor = null;
    }
  }

}
