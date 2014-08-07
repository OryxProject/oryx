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

package com.cloudera.oryx.ml.serving.als;

import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

//@ApplicationPath("")
public  class ALSApplication extends ResourceConfig {
  public ALSApplication() {
    packages("com.cloudera.oryx.serving.als");
    /*
    Set<Class<?>> s = new HashSet<>();
    s.add(AllItemIDs.class);
    s.add(Because.class);
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
    return s;
    */
  }
}


