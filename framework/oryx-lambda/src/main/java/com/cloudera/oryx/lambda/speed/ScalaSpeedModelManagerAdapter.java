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

package com.cloudera.oryx.lambda.speed;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import scala.collection.JavaConversions;

import com.cloudera.oryx.lambda.KeyMessage;

public final class ScalaSpeedModelManagerAdapter<K,M,U> implements SpeedModelManager<K,M,U> {

  private final ScalaSpeedModelManager<K,M,U> scalaManager;

  public ScalaSpeedModelManagerAdapter(ScalaSpeedModelManager<K,M,U> scalaManager) {
    Preconditions.checkNotNull(scalaManager);
    this.scalaManager = scalaManager;
  }

  @Override
  public void consume(Iterator<KeyMessage<String, U>> updateIterator) {
    scalaManager.consume(JavaConversions.asScalaIterator(updateIterator));
  }

  @Override
  public Iterable<U> buildUpdates(JavaPairRDD<K, M> newData) {
    return JavaConversions.asJavaIterable(scalaManager.buildUpdates(newData.rdd()));
  }

  @Override
  public void close() {
    scalaManager.close();
  }

}
