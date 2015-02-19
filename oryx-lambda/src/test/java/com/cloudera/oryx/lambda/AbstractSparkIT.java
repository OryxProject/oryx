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
import org.junit.After;
import org.junit.Before;

import com.cloudera.oryx.common.OryxTest;

public abstract class AbstractSparkIT extends OryxTest {

  private JavaSparkContext javaSparkContext;

  @Before
  public final void setUp() {
    SparkConf sparkConf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("SparkKMeansTest");

    javaSparkContext = new JavaSparkContext(sparkConf);
  }

  @After
  public final void tearDown() {
    javaSparkContext.close();
    javaSparkContext = null;
  }

  protected final JavaSparkContext getJavaSparkContext() {
    return javaSparkContext;
  }
}
