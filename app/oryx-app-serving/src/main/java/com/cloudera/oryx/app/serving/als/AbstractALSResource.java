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

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.Lists;
import net.openhft.koloboke.function.ObjDoubleToDoubleFunction;

import com.cloudera.oryx.app.als.Rescorer;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;

/**
 * Superclass of all Serving Layer ALS application endpoints.
 */
public abstract class AbstractALSResource extends AbstractOryxResource {

  final ALSServingModel getALSServingModel() throws OryxServingException {
    return (ALSServingModel) getServingModel();
  }

  static <T> List<T> selectedSublist(List<T> values, int howMany, int offset) {
    if (values.size() < offset) {
      return Collections.emptyList();
    }
    return values.subList(offset, Math.min(offset + howMany, values.size()));
  }

  static List<IDValue> toIDValueResponse(List<Pair<String,Double>> pairs,
                                         int howMany,
                                         int offset) {
    List<Pair<String,Double>> sublist = selectedSublist(pairs, howMany, offset);
    return Lists.transform(sublist, idDot -> new IDValue(idDot.getFirst(), idDot.getSecond()));
  }

  static ObjDoubleToDoubleFunction<String> buildRescoreFn(Rescorer rescorer) {
    return rescorer::rescore;
  }

  static Predicate<String> buildRescorerPredicate(Rescorer rescorer) {
    return id -> !rescorer.isFiltered(id);
  }

}
