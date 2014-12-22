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

package com.cloudera.oryx.common.text;

import java.io.IOException;
import java.util.Iterator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Text and parsing related utility methods.
 */
public final class TextUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.withSkipHeaderRecord();
  private static final String[] EMPTY_STRING = { "" };

  private TextUtils() {}

  /**
   * @param csv line of CSV-formatted text
   * @return delimited strings, parsed according to RFC 4180
   */
  public static String[] parseCSV(String csv) {
    Iterator<CSVRecord> records = null;
    if (csv != null) {
      try {
				records = CSVParser.parse(csv, CSV_FORMAT).iterator();
			} catch (IOException e) {
				throw new IllegalStateException(e); // Can't happen
			}
    }
    if (records != null && records.hasNext()) {
      return Iterators.toArray(records.next().iterator(), String.class);
    } else {
      return EMPTY_STRING;
    }
  }

  /**
   * @param json line of JSON text
   * @return delimited strings, the elements of the JSON array
   * @throws IOException if JSON parsing fails
   */
  public static String[] parseJSONArray(String json) throws IOException {
    if (json != null) {
      return MAPPER.readValue(json, String[].class);
    } else {
      return EMPTY_STRING;
    }
  }

}
