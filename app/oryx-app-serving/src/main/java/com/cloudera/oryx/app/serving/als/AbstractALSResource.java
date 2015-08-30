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
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import net.openhft.koloboke.function.ObjDoubleToDoubleFunction;
import net.openhft.koloboke.function.Predicate;

import com.cloudera.oryx.api.serving.ServingModelManager;
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

  private boolean isLoaded;

  final ALSServingModel getALSServingModel() throws OryxServingException {
    ServingModelManager<?> modelManager = getServingModelManager();
    ALSServingModel alsServingModel = (ALSServingModel) modelManager.getModel();
    if (isLoaded) {
      return alsServingModel;
    }
    if (alsServingModel != null) {
      double minModelLoadFraction = modelManager.getConfig().getDouble("oryx.serving.min-model-load-fraction");
      if (alsServingModel.getFractionLoaded() >= minModelLoadFraction) {
        isLoaded = true;
      }
    }
    if (isLoaded) {
      return alsServingModel;
    } else {
      throw new OryxServingException(Response.Status.SERVICE_UNAVAILABLE);
    }
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
    return Lists.transform(sublist,
        new Function<Pair<String,Double>,IDValue>() {
          @Override
          public IDValue apply(Pair<String,Double> idDot) {
            return new IDValue(idDot.getFirst(), idDot.getSecond());
          }
        });
  }

  static ObjDoubleToDoubleFunction<String> buildRescoreFn(final Rescorer rescorer) {
    return new ObjDoubleToDoubleFunction<String>() {
      @Override
      public double applyAsDouble(String ID, double score) {
        return rescorer.rescore(ID, score);
      }
    };
  }

  static Predicate<String> buildRescorerPredicate(final Rescorer rescorer) {
    return new Predicate<String>() {
      @Override
      public boolean test(String ID) {
        return !rescorer.isFiltered(ID);
      }
    };
  }

}
