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

package com.cloudera.oryx.lambda.batch;

import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Translates a key and message in {@link Writable}s format to value objects.
 *
 * @see ValueToWritableFunction
 */
final class WritableToValueFunction<K,M>
    implements PairFunction<Tuple2<Writable,Writable>,K,M> {

  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;
  private transient ValueWritableConverter<K> keyConverter;
  private transient ValueWritableConverter<M> messageConverter;

  WritableToValueFunction(Class<K> keyClass,
                          Class<M> messageClass,
                          Class<? extends Writable> keyWritableClass,
                          Class<? extends Writable> messageWritableClass) {
    this.keyClass = keyClass;
    this.messageClass = messageClass;
    this.keyWritableClass = keyWritableClass;
    this.messageWritableClass = messageWritableClass;
    initConverters();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initConverters();
  }

  private void initConverters() {
    keyConverter = new ValueWritableConverter<>(keyClass, keyWritableClass);
    messageConverter = new ValueWritableConverter<>(messageClass, messageWritableClass);
  }

  @Override
  public Tuple2<K,M> call(Tuple2<Writable,Writable> keyMessage) {
    return new Tuple2<>(keyConverter.fromWritable(keyMessage._1()),
                        messageConverter.fromWritable(keyMessage._2()));
  }

}
