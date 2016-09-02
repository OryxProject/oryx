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

package com.cloudera.oryx.app.serving.als;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.common.collection.Pair;

/**
 * Superclass of all Serving Layer ALS application endpoints.
 */
abstract class AbstractALSResource extends AbstractOryxResource {

  /** Somewhat arbitrarily cap the number of results that can be requested. */
  private static final int MAX_RESULTS = 100000;

  final ALSServingModel getALSServingModel() throws OryxServingException {
    return (ALSServingModel) getServingModel();
  }

  static int checkHowManyOffset(int howMany, int offset) throws OryxServingException {
    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be nonnegative");
    check(howMany <= MAX_RESULTS && offset <= MAX_RESULTS && howMany + offset <= MAX_RESULTS,
          "howMany + offset is too large");
    return howMany + offset;
  }

  static List<IDValue> toIDValueResponse(Stream<Pair<String,Double>> pairs,
                                         int howMany,
                                         int offset) {
    return pairs.skip(offset).limit(howMany)
        .map(idDot -> new IDValue(idDot.getFirst(), idDot.getSecond()))
        .collect(Collectors.toList());
  }

}
