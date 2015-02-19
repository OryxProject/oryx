/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.cloudera.oryx.common.OryxTest;

public abstract class AbstractSparkIT extends OryxTest {

  private static JavaSparkContext javaSparkContext;

  @BeforeClass
  public static void setUp() {
    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkIT");
    javaSparkContext = new JavaSparkContext(sparkConf);
  }

  @AfterClass
  public static void tearDown() {
    if (javaSparkContext != null) {
      javaSparkContext.close();
      javaSparkContext = null;
    }
  }

  protected static JavaSparkContext getJavaSparkContext() {
    return javaSparkContext;
  }
}
