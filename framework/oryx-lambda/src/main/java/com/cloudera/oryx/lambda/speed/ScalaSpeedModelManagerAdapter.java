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
import java.util.Objects;

import org.apache.spark.api.java.JavaPairRDD;
import scala.collection.JavaConversions;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.speed.ScalaSpeedModelManager;
import com.cloudera.oryx.api.speed.SpeedModelManager;

/**
 * Adapts a {@link ScalaSpeedModelManager} to be a Java {@link SpeedModelManager}.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
public final class ScalaSpeedModelManagerAdapter<K,M,U> implements SpeedModelManager<K,M,U> {

  private final ScalaSpeedModelManager<K,M,U> scalaManager;

  public ScalaSpeedModelManagerAdapter(ScalaSpeedModelManager<K,M,U> scalaManager) {
    Objects.requireNonNull(scalaManager);
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
