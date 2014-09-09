/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.ml.serving.IDCount;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /mostPopularItems(?howMany=n)(&offset=o)}
 * and in turn calls {link OryxRecommender#mostPopularItems(int)}.
 * {@code offset} is an offset into the entire list of results; {@code howMany} is the desired
 * number of results to return from there. For example, {@code offset=30} and {@code howMany=5}
 * will cause the implementation to retrieve 35 results internally and output the last 5.
 * If {@code howMany} is not specified, defaults to {link AbstractALSServlet#DEFAULT_HOW_MANY}.
 * {@code offset} defaults to 0.</p>
 *
 * <p>Output is as in {@link Recommend}.</p>
 */
@Path("/mostPopularItems")
public final class MostPopularItems extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<IDCount> get(@DefaultValue("10") @QueryParam("howMany") int howMany,
                           @DefaultValue("0") @QueryParam("offset") int offset) {

    ALSServingModel model = getALSServingModel();
    ObjectIntMap<String> itemCounts = model.getItemCounts();

    Iterable<Pair<String,Integer>> countPairs =
        Iterables.transform(itemCounts,
            new Function<ObjectIntCursor<String>, Pair<String,Integer>>() {
              @Override
              public Pair<String,Integer> apply(ObjectIntCursor<String> input) {
                return new Pair<>(input.key, input.value);
              }
            });

    List<Pair<String,Integer>> allTopCountPairs =
        Ordering.from(PairComparators.<Integer>bySecond()).greatestOf(countPairs, howMany + offset);
    List<Pair<String,Integer>> topCountPairs =
        selectedSublist(allTopCountPairs, howMany, offset);

    return Lists.transform(topCountPairs,
        new Function<Pair<String, Integer>, IDCount>() {
          @Override
          public IDCount apply(Pair<String,Integer> idCount) {
            return new IDCount(idCount.getFirst(), idCount.getSecond());
          }
        });
  }

}
