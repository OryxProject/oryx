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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /similarity/[itemID1](/[itemID2]/...)(?howMany=n)(&offset=o)}.</p>
 *
 * <p>Results are items that are most similar to a given item.
 * Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean more relevant to recommendation.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior, and output, are as in {@link Recommend}.</p>
 *
 * <p>Default output is CSV format, containing {@code id,value} per line.
 * JSON format can also be selected by an appropriate {@code Accept} header. It returns
 * an array of similarities, each of which has an "id" and "value" entry, like
 * [{"id":"I2","value":0.141348009071816},...]</p>
 */
@Path("/similarity")
public final class Similarity extends AbstractALSResource {

  @GET
  @Path("{itemID : .+}")
  @Produces({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("itemID") List<PathSegment> pathSegmentsList,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset,
      @QueryParam("rescorerParams") List<String> rescorerParams) throws OryxServingException {

    check(!pathSegmentsList.isEmpty(), "Need at least 1 item to determine similarity");
    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be non-negative");

    ALSServingModel alsServingModel = getALSServingModel();
    float[][] itemFeatureVectors = new float[pathSegmentsList.size()][];
    ObjectSet<String> knownItems = new ObjectOpenHashSet<>();

    for (int i = 0; i < itemFeatureVectors.length; i++) {
      String itemID = pathSegmentsList.get(i).getPath();
      float[] itemVector = alsServingModel.getItemVector(itemID);
      checkExists(itemVector != null, itemID);
      itemFeatureVectors[i] = itemVector;
      knownItems.add(itemID);
    }

    List<Pair<String,Double>> topIDCosines = alsServingModel.topN(
        new CosineAverageFunction(itemFeatureVectors),
        howMany + offset,
        new NotKnownPredicate(knownItems));

    return toIDValueResponse(topIDCosines, howMany, offset);
  }
}
