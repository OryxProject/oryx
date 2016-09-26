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

package com.cloudera.oryx.app.kmeans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunction;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.SquaredEuclidean;
import org.dmg.pmml.clustering.Cluster;
import org.dmg.pmml.clustering.ClusteringField;
import org.dmg.pmml.clustering.ClusteringModel;
import org.junit.Test;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class KMeansPMMLUtilsTest extends OryxTest {

  private static final int NUM_CLUSTERS = 3;
  private static final int CLUSTER_SIZE = 2;
  private static final int NUM_DIMENSIONS = 2;

  @Test
  public void testClustering() {
    PMML pmml = buildDummyClusteringModel();
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.feature-names", "[\"x\",\"y\"]");
    overlayConfig.put("oryx.input-schema.num-features", 2);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema schema = new InputSchema(config);
    KMeansPMMLUtils.validatePMMLVsSchema(pmml, schema);
  }

  @Test
  public void testPMMLToClusters() {
    PMML pmml = buildDummyClusteringModel();
    List<ClusterInfo> clusterInfoList = KMeansPMMLUtils.read(pmml);
    assertEquals(NUM_CLUSTERS, clusterInfoList.size());
    assertEquals(NUM_DIMENSIONS, clusterInfoList.get(0).getCenter().length);
    assertEquals(CLUSTER_SIZE, clusterInfoList.get(1).getCount());
  }

  public static PMML buildDummyClusteringModel() {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    List<DataField> dataFields = new ArrayList<>();
    dataFields.add(new DataField(FieldName.create("x"), OpType.CONTINUOUS, DataType.DOUBLE));
    dataFields.add(new DataField(FieldName.create("y"), OpType.CONTINUOUS, DataType.DOUBLE));
    DataDictionary dataDictionary =
        new DataDictionary(dataFields).setNumberOfFields(dataFields.size());
    pmml.setDataDictionary(dataDictionary);

    List<MiningField> miningFields = new ArrayList<>();
    MiningField xMF = new MiningField(FieldName.create("x"))
        .setOpType(OpType.CONTINUOUS).setUsageType(MiningField.UsageType.ACTIVE);
    miningFields.add(xMF);
    MiningField yMF = new MiningField(FieldName.create("y"))
        .setOpType(OpType.CONTINUOUS).setUsageType(MiningField.UsageType.ACTIVE);
    miningFields.add(yMF);
    MiningSchema miningSchema = new MiningSchema(miningFields);

    List<ClusteringField> clusteringFields = new ArrayList<>();
    clusteringFields.add(new ClusteringField(
        FieldName.create("x")).setCenterField(ClusteringField.CenterField.TRUE));
    clusteringFields.add(new ClusteringField(
        FieldName.create("y")).setCenterField(ClusteringField.CenterField.TRUE));

    List<Cluster> clusters = new ArrayList<>();
    clusters.add(new Cluster().setId("0").setSize(1).setArray(AppPMMLUtils.toArray(1.0, 0.0)));
    clusters.add(new Cluster().setId("1").setSize(2).setArray(AppPMMLUtils.toArray(2.0, -1.0)));
    clusters.add(new Cluster().setId("2").setSize(3).setArray(AppPMMLUtils.toArray(-1.0, 0.0)));

    pmml.addModels(new ClusteringModel(
        MiningFunction.CLUSTERING,
        ClusteringModel.ModelClass.CENTER_BASED,
        clusters.size(),
        miningSchema,
        new ComparisonMeasure(ComparisonMeasure.Kind.DISTANCE).setMeasure(new SquaredEuclidean()),
        clusteringFields,
        clusters));

    return pmml;
  }

}
