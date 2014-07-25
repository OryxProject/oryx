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

package com.cloudera.oryx.ml.als;

import java.util.List;

import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.ml.MLUpdate;

public final class ALSUpdate extends MLUpdate<String> {

  public ALSUpdate(Config config) {
    super(config);
  }

  @Override
  public PMML buildModel(JavaSparkContext sparkContext,
                         JavaRDD<String> trainData,
                         List<Number> hyperParameters,
                         Path candidatePath) {
    return null; // TODO
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData) {
    // TODO
    return 0;
  }

}
