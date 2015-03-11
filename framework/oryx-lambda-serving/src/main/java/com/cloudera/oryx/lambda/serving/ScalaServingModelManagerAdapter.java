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

package com.cloudera.oryx.lambda.serving;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import scala.collection.JavaConversions;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.serving.ScalaServingModelManager;
import com.cloudera.oryx.api.serving.ServingModelManager;

public final class ScalaServingModelManagerAdapter<U> implements ServingModelManager<U> {

  private final ScalaServingModelManager<U> scalaManager;

  public ScalaServingModelManagerAdapter(ScalaServingModelManager<U> scalaManager) {
    Preconditions.checkNotNull(scalaManager);
    this.scalaManager = scalaManager;
  }

  @Override
  public void consume(Iterator<KeyMessage<String,U>> updateIterator) {
    scalaManager.consume(JavaConversions.asScalaIterator(updateIterator));
  }

  @Override
  public Object getModel() {
    return scalaManager.getModel();
  }

  @Override
  public void close() {
    scalaManager.close();
  }

}
