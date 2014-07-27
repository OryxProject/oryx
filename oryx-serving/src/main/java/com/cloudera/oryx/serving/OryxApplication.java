/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.serving;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("")
public class OryxApplication extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> s = new HashSet<>();
    s.add(com.cloudera.oryx.serving.web.Add.class);
    s.add(com.cloudera.oryx.serving.web.AllItemIDs.class);
    s.add(com.cloudera.oryx.serving.web.Assign.class);
    s.add(com.cloudera.oryx.serving.web.Because.class);
    s.add(com.cloudera.oryx.serving.web.Classify.class);
    s.add(com.cloudera.oryx.serving.web.DistanceToNearest.class);
    s.add(com.cloudera.oryx.serving.web.Estimate.class);
    s.add(com.cloudera.oryx.serving.web.EstimateForAnonymous.class);
    s.add(com.cloudera.oryx.serving.web.Ingest.class);
    s.add(com.cloudera.oryx.serving.web.Ingest.class);
    s.add(com.cloudera.oryx.serving.web.MostPopularItems.class);
    s.add(com.cloudera.oryx.serving.web.MostSurprising.class);
    s.add(com.cloudera.oryx.serving.web.PopularRepresentativeItems.class);
    s.add(com.cloudera.oryx.serving.web.Preference.class);
    s.add(com.cloudera.oryx.serving.web.Ready.class);
    s.add(com.cloudera.oryx.serving.web.Recommend.class);
    s.add(com.cloudera.oryx.serving.web.RecommendResponse.class);
    s.add(com.cloudera.oryx.serving.web.RecommendToAnonymous.class);
    s.add(com.cloudera.oryx.serving.web.RecommendToMany.class);
    s.add(com.cloudera.oryx.serving.web.Similarity.class);
    s.add(com.cloudera.oryx.serving.web.SimilarityToItem.class);
    s.add(com.cloudera.oryx.serving.web.Train.class);
    return s;
  }
}


