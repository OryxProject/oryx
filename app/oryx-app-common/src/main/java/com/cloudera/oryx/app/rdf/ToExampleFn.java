/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.rdf;

import java.util.Arrays;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.Feature;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;

/**
 * Transforms input as a raw list of strings into an {@link Example}
 * representation.
 */
public final class ToExampleFn implements Function<String[],Example> {

  private static final Logger log = LoggerFactory.getLogger(ToExampleFn.class);

  private final InputSchema inputSchema;
  private final CategoricalValueEncodings valueEncodings;

  public ToExampleFn(InputSchema inputSchema, CategoricalValueEncodings valueEncodings) {
    this.inputSchema = inputSchema;
    this.valueEncodings = valueEncodings;
  }

  @Override
  public Example call(String[] data) {
    try {
      Feature[] features = new Feature[data.length];
      Feature target = null;
      for (int featureIndex = 0; featureIndex < data.length; featureIndex++) {
        Feature feature = null;
        String dataAtIndex = data[featureIndex];
        boolean isTarget = inputSchema.isTarget(featureIndex);
        if (isTarget && dataAtIndex.isEmpty()) {
          feature = null;
        } else if (inputSchema.isNumeric(featureIndex)) {
          feature = NumericFeature.forValue(Double.parseDouble(dataAtIndex));
        } else if (inputSchema.isCategorical(featureIndex)) {
          int encoding = valueEncodings.getValueEncodingMap(featureIndex).get(dataAtIndex);
          feature = CategoricalFeature.forEncoding(encoding);
        }
        if (isTarget) {
          target = feature;
        } else {
          features[featureIndex] = feature;
        }
      }
      return new Example(target, features);
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
      log.warn("Bad input: {}", Arrays.toString(data));
      throw e;
    }
  }

}
