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

package com.cloudera.oryx.lambda;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import scala.Tuple2;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class ValueToWritableFunctionTest extends OryxTest {

  @Test
  public void testFunction() {
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("batch.storage.key-writable-class", IntWritable.class.getName());
    overlayConfig.put("batch.storage.message-writable-class", LongWritable.class.getName());
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSerializationConfig serializationConfig = new InputSerializationConfig(config);
    ValueToWritableFunction<Integer,Long> function =
        new ValueToWritableFunction<>(Integer.class, Long.class, serializationConfig);
    Tuple2<Integer,Long> in = new Tuple2<>(3, 4L);
    Tuple2<Writable,Writable> out = function.call(in);
    assertEquals(new IntWritable(3), out._1());
    assertEquals(new LongWritable(4L), out._2());
  }

}