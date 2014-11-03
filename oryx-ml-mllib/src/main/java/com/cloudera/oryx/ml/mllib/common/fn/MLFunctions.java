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

package com.cloudera.oryx.ml.mllib.common.fn;

import java.io.IOException;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.Function;

public final class MLFunctions {

  private MLFunctions() {}

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern COMMA = Pattern.compile(",");

  /**
   * Parses a CSV or JSON array to String[]
   */
  public static final Function<String,String[]> PARSE_FN =
      new Function<String,String[]>() {
        @Override
        public String[] call(String line) throws IOException {
          // Hacky, but effective way of differentiating simple CSV from JSON array
          String[] result;
          if (line.endsWith("]")) {
            // JSON
            result = MAPPER.readValue(line, String[].class);
          } else {
            // CSV
            result = COMMA.split(line);
          }
          return result;
        }
      };
}
