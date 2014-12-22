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

package com.cloudera.oryx.ml.mllib.rdf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.common.fn.MLFunctions;
import com.cloudera.oryx.ml.param.HyperParamRange;
import com.cloudera.oryx.ml.param.HyperParamRanges;
import com.cloudera.oryx.ml.schema.InputSchema;

public final class RDFUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(RDFUpdate.class);

  private final int numTrees;
  private final List<HyperParamRange> hyperParamRanges;
  private final InputSchema inputSchema;


  public RDFUpdate(Config config) {
    super(config);
    numTrees = config.getInt("oryx.rdf.hyperparams.num-trees");
    Preconditions.checkArgument(numTrees >= 1);
    hyperParamRanges = Arrays.asList(
        HyperParamRanges.fromConfig(config, "oryx.rdf.hyperparams.max-split-candidates"),
        HyperParamRanges.fromConfig(config, "oryx.rdf.hyperparams.max-depth"));

    inputSchema = new InputSchema(config);
  }

  @Override
  public List<HyperParamRange> getHyperParameterRanges() {
    return hyperParamRanges;
  }

  @Override
  public PMML buildModel(JavaSparkContext sparkContext,
                         JavaRDD<String> trainData,
                         List<Number> hyperParameters,
                         Path candidatePath) {
    int maxSplitCandidates = hyperParameters.get(0).intValue();
    int maxDepth = hyperParameters.get(0).intValue();
    Preconditions.checkArgument(maxSplitCandidates > 0);
    Preconditions.checkArgument(maxDepth > 0);

    JavaRDD<LabeledPoint> trainPointData = parsedToRatingRDD(trainData.map(MLFunctions.PARSE_FN));

    // TODO

    return null;
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData) {
    return 0;
  }

  private JavaRDD<LabeledPoint> parsedToRatingRDD(JavaRDD<String[]> parsedRDD) {

    List<Set<String>> distinctCategoricalValues = new ArrayList<>(inputSchema.getNumFeatures());

    // TODO

    return null;
  }

}