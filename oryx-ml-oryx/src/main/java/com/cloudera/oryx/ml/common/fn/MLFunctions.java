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

package com.cloudera.oryx.ml.common.fn;

import java.io.IOException;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import com.cloudera.oryx.common.text.TextUtils;

/**
 * Contains some reusable functions that are useful for ML-related apps from this project itself,
 * and based on MLlib too.
 */
public final class MLFunctions {

  private MLFunctions() {}

  /**
   * Parses a CSV or JSON array to String[]
   */
  public static final Function<String,String[]> PARSE_FN =
      new Function<String,String[]>() {
        @Override
        public String[] call(String line) throws IOException {
          // Hacky, but effective way of differentiating simple CSV from JSON array
          if (line.startsWith("[") && line.endsWith("]")) {
            // JSON
            return TextUtils.parseJSONArray(line);
          } else {
            // CSV
            return TextUtils.parseCSV(line);
          }
        }
      };

  /**
   * Parses as if with PARSE_FN and returns fourth field as timestamp
   */
  public static final Function<String,Long> TO_TIMESTAMP_FN =
      new Function<String,Long>() {
        @Override
        public Long call(String line) throws Exception {
          return Long.valueOf(PARSE_FN.call(line)[3]);
        }
      };

  public static <K> Function<Tuple2<K,Double>,Boolean> notNaNValue() {
    return new Function<Tuple2<K,Double>,Boolean>() {
                  @Override
                  public Boolean call(Tuple2<K,Double> kv) {
                    return !Double.isNaN(kv._2());
                  }
                };
  }

  public static final Function<Iterable<Double>,Double> SUM_WITH_NAN =
      new Function<Iterable<Double>, Double>() {
        @Override
        public Double call(Iterable<Double> orderedStrengths) {
          double finalStrength = Double.NaN;
          for (double strength : orderedStrengths) {
            if (Double.isNaN(finalStrength)) {
              finalStrength = strength;
            } else {
              // If strength is NaN, this will make the tally NaN
              finalStrength += strength;
            }
          }
          return finalStrength;
        }
      };

}
