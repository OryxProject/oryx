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

import com.cloudera.oryx.serving.web.Add;
import com.cloudera.oryx.serving.web.AllItemIDs;
import com.cloudera.oryx.serving.web.Assign;
import com.cloudera.oryx.serving.web.Because;
import com.cloudera.oryx.serving.web.Classify;
import com.cloudera.oryx.serving.web.DistanceToNearest;
import com.cloudera.oryx.serving.web.Estimate;
import com.cloudera.oryx.serving.web.EstimateForAnonymous;
import com.cloudera.oryx.serving.web.Ingest;
import com.cloudera.oryx.serving.web.MostPopularItems;
import com.cloudera.oryx.serving.web.MostSurprising;
import com.cloudera.oryx.serving.web.PopularRepresentativeItems;
import com.cloudera.oryx.serving.web.Preference;
import com.cloudera.oryx.serving.web.Ready;
import com.cloudera.oryx.serving.web.Recommend;
import com.cloudera.oryx.serving.web.RecommendResponse;
import com.cloudera.oryx.serving.web.RecommendToAnonymous;
import com.cloudera.oryx.serving.web.RecommendToMany;
import com.cloudera.oryx.serving.web.Similarity;
import com.cloudera.oryx.serving.web.SimilarityToItem;
import com.cloudera.oryx.serving.web.Train;

@ApplicationPath("")
public class OryxApplication extends Application {
  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> s = new HashSet<>();
    s.add(Add.class);
    s.add(AllItemIDs.class);
    s.add(Assign.class);
    s.add(Because.class);
    s.add(Classify.class);
    s.add(DistanceToNearest.class);
    s.add(Estimate.class);
    s.add(EstimateForAnonymous.class);
    s.add(Ingest.class);
    s.add(MostPopularItems.class);
    s.add(MostSurprising.class);
    s.add(PopularRepresentativeItems.class);
    s.add(Preference.class);
    s.add(Ready.class);
    s.add(Recommend.class);
    s.add(RecommendResponse.class);
    s.add(RecommendToAnonymous.class);
    s.add(RecommendToMany.class);
    s.add(Similarity.class);
    s.add(SimilarityToItem.class);
    s.add(Train.class);
    return s;
  }
}


