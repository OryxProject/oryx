/*
* Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
*
* Cloudera, Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"). You may not use this file except in
* compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for
* the specific language governing permissions and limitations under the
* License.
*/

package com.cloudera.oryx.app.speed.kmeans;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.common.OryxTest;

public final class KMeansSpeedModelTest extends OryxTest {

  @Test
  public void update() {
    List<ClusterInfo> clusters = new ArrayList<>();
    clusters.add(new ClusterInfo(0, new double[]{-1.0, 2.0}, 2));
    clusters.add(new ClusterInfo(1, new double[]{1.0, 3.0}, 3));
    clusters.add(new ClusterInfo(2, new double[]{2.0, -1.0}, 4));

    KMeansSpeedModel model = new KMeansSpeedModel(clusters);
    ClusterInfo cluster = model.getCluster(1);
    cluster.update(new double[]{-1.0,-1.0}, 3);

    assertEquals(1, cluster.getID());
    assertEquals(6, cluster.getCount());
    assertArrayEquals(new double[]{0.0,1.0}, cluster.getCenter());
  }
}