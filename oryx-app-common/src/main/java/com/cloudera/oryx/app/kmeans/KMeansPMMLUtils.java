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
import java.util.List;

import com.google.common.base.Preconditions;
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.text.TextUtils;

public final class KMeansPMMLUtils {

  private KMeansPMMLUtils() {}

  /**
   * Validates that the encoded PMML model received matches expected schema.
   *
   * @param pmml {@link org.dmg.pmml.PMML} encoding of KMeans Clustering
   * @param schema expected schema attributes of KMeans Clustering
   */
  public static void validatePMMLVsSchema(PMML pmml, InputSchema schema) {
    List<Model> models = pmml.getModels();
    Preconditions.checkArgument(models.size() == 1,
        "Should have exactly one model, but had %s", models.size());

    Model model = models.get(0);
    Preconditions.checkArgument(model instanceof ClusteringModel);
    Preconditions.checkArgument(model.getFunctionName() == MiningFunctionType.CLUSTERING);

    DataDictionary dictionary = pmml.getDataDictionary();
    Preconditions.checkArgument(
        schema.getFeatureNames().equals(AppPMMLUtils.getFeatureNames(dictionary)),
        "Feature names in schema don't match names in PMML");

    MiningSchema miningSchema = model.getMiningSchema();
    Preconditions.checkArgument(schema.getFeatureNames().equals(
        AppPMMLUtils.getFeatureNames(miningSchema)));

  }

  /**
   * @param pmml PMML representation of Clusters
   * @return List of {@link ClusterInfo}
   */
  public static List<ClusterInfo> read(PMML pmml) {
    List<Model> models = pmml.getModels();
    Model model = models.get(0);

    Preconditions.checkArgument(model instanceof ClusteringModel);
    ClusteringModel clusteringModel = (ClusteringModel) model;

    List<Cluster> clusters = clusteringModel.getClusters();
    List<ClusterInfo> clusterInfoList = new ArrayList<>(clusters.size());

    for (Cluster cluster : clusters) {
      String[] tokens = TextUtils.parseDelimited(cluster.getArray().getValue(), ' ');
      ClusterInfo clusterInfo = new ClusterInfo(Integer.parseInt(cluster.getId()),
                                                VectorMath.parseVector(tokens),
                                                cluster.getSize());
      clusterInfoList.add(clusterInfo);
    }

    return clusterInfoList;
  }

}
