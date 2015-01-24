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

package com.cloudera.oryx.app.kmeans;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.SquaredEuclidean;
import org.junit.Test;

import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class KMeansPMMLUtilsTest extends OryxTest {

  @Test
  public void testClustering() {
    PMML pmml = buildDummyClusteringModel();
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.kmeans.hyperparams.k", 3);
    overlayConfig.put("oryx.input-schema.num-features", 3);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema schema = new InputSchema(config);
    KMeansPMMLUtils.validatePMMLVsSchema(pmml, schema);
  }

  public static PMML buildDummyClusteringModel() {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    DataDictionary dataDictionary = new DataDictionary();
    dataDictionary.setNumberOfFields(3);
    DataField dataField1 = new DataField(new FieldName("0"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(dataField1);
    DataField dataField2 = new DataField(new FieldName("1"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(dataField2);
    DataField dataField3 = new DataField(new FieldName("2"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(dataField3);
    pmml.setDataDictionary(dataDictionary);

    MiningSchema miningSchema = new MiningSchema();
    MiningField miningField1 = new MiningField(new FieldName("0"));
    miningField1.setOptype(OpType.CONTINUOUS);
    miningField1.setUsageType(FieldUsageType.ACTIVE);
    miningSchema.getMiningFields().add(miningField1);
    MiningField miningField2 = new MiningField(new FieldName("1"));
    miningField2.setOptype(OpType.CONTINUOUS);
    miningField2.setUsageType(FieldUsageType.ACTIVE);
    miningSchema.getMiningFields().add(miningField2);
    MiningField miningField3 = new MiningField(new FieldName("2"));
    miningField3.setOptype(OpType.CONTINUOUS);
    miningField3.setUsageType(FieldUsageType.ACTIVE);
    miningSchema.getMiningFields().add(miningField3);

    ClusteringModel clusteringModel =
        new ClusteringModel(miningSchema,
            new ComparisonMeasure(ComparisonMeasure.Kind.DISTANCE).withMeasure(new SquaredEuclidean()),
            MiningFunctionType.CLUSTERING, ClusteringModel.ModelClass.CENTER_BASED, 3)
            .withAlgorithmName("K-Means||")
            .withNumberOfClusters(3);

    pmml.getModels().add(clusteringModel);

    return pmml;
  }
}
