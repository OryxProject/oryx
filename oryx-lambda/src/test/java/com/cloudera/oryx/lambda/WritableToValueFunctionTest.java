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

import com.cloudera.oryx.common.OryxTest;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import scala.Tuple2;

public final class WritableToValueFunctionTest extends OryxTest {

  @Test
  public void testFunction() {
    WritableToValueFunction<String,String> function =
        new WritableToValueFunction<>(String.class, String.class, Text.class, Text.class);
    Tuple2<Writable,Writable> in = new Tuple2<Writable,Writable>(new Text("bizz"), new Text("buzz"));
    Tuple2<String,String> out = function.call(in);
    assertEquals("bizz", out._1());
    assertEquals("buzz", out._2());
  }

}