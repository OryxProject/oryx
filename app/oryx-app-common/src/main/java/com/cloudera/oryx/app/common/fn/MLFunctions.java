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

package com.cloudera.oryx.app.common.fn;

import java.io.IOException;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.text.TextUtils;

/**
 * Contains some reusable functions that are useful for ML-related apps from this project itself,
 * and based on MLlib too.
 */
public final class MLFunctions {

  private static final Logger log = LoggerFactory.getLogger(MLFunctions.class);

  private MLFunctions() {}

  /**
   * Parses a CSV or JSON array to String[]
   */
  public static final Function<String,String[]> PARSE_FN = line -> {
      try {
        // Hacky, but effective way of differentiating simple CSV from JSON array
        if (line.startsWith("[") && line.endsWith("]")) {
          // JSON
          return TextUtils.parseJSONArray(line);
        } else {
          // CSV
          return TextUtils.parseDelimited(line, ',');
        }
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException | IOException e) {
        log.warn("Bad input: {}", line);
        throw e;
      }
    };

  /**
   * Parses as if with PARSE_FN and returns fourth field as timestamp
   */
  public static final Function<String,Long> TO_TIMESTAMP_FN = line -> {
      try {
        return Long.valueOf(PARSE_FN.call(line)[3]);
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", line);
        throw e;
      }
    };

  public static final Function<Iterable<Double>,Double> SUM_WITH_NAN = orderedStrengths -> {
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
    };

}
