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
import java.io.StringWriter;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

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
    return parseDelimited(csv, ',');
  }

  /**
   * @param delimited line of delimited text
   * @param delimiter delimiter to split fields on
   * @return delimited strings, parsed according to RFC 4180 but with the given delimiter
   */
  public static String[] parseDelimited(String delimited, char delimiter) {
    CSVFormat format = formatForDelimiter(delimiter, false);
    Iterator<CSVRecord> records;
    try {
      records = CSVParser.parse(delimited, format).iterator();
    } catch (IOException e) {
      throw new IllegalStateException(e); // Can't happen
    }
    if (records.hasNext()) {
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
    return MAPPER.readValue(json, String[].class);
  }

  /**
   * @param elements values to join by comma to make one line of CSV
   * @return one line of CSV, with RFC 4180 escaping (values with comma are quoted; double-quotes
   *  are escaped by doubling)
   */
  public static String joinCSV(Iterable<?> elements) {
    return joinDelimited(elements, ',');
  }

  /**
   * @param elements values to join by the delimiter to make one line of text
   * @param delimiter delimiter to put between fields
   * @return one line of text, with RFC 4180 escaping (values with comma are quoted; double-quotes
   *  are escaped by doubling) and using the given delimiter
   */
  public static String joinDelimited(Iterable<?> elements, char delimiter) {
    return joinDelimited(elements, delimiter, false);
  }

  /**
   * @param elements values to join by the delimiter to make one line of text
   * @param delimiter delimiter to put between fields
   * @param noQuote force no quoting. Useful in some cases where it's known that quoting
   *  isn't needed
   * @return one line of text, with RFC 4180 escaping (values with comma are quoted; double-quotes
   *  are escaped by doubling, unless {@code noQuote} is set) and using the given delimiter
   */
  public static String joinDelimited(Iterable<?> elements, char delimiter, boolean noQuote) {
    CSVFormat format = formatForDelimiter(delimiter, noQuote);
    StringWriter out = new StringWriter();
    try (CSVPrinter printer = new CSVPrinter(out, format)) {
      for (Object element : elements) {
        printer.print(element);
      }
      printer.flush();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return out.toString();
  }

  public static String joinJSON(Iterable<?> elements) {
    try {
      return MAPPER.writeValueAsString(elements);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static CSVFormat formatForDelimiter(char delimiter, boolean noQuote) {
    CSVFormat format = CSV_FORMAT;
    if (delimiter != format.getDelimiter()) {
      format = format.withDelimiter(delimiter);
    }
    if (noQuote) {
      format = format.withEscape('\\').withQuoteMode(QuoteMode.NONE);
    }
    return format;
  }

}
