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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  private static final CSVFormat CSV_FORMAT =
      CSVFormat.RFC4180.withSkipHeaderRecord().withEscape('\\');
  private static final String[] EMPTY_STRING = { "" };
  private static final Pattern TWO_DOUBLE_QUOTE_ESC = Pattern.compile("\"\"", Pattern.LITERAL);
  private static final String SLASH_QUOTE_ESC = Matcher.quoteReplacement("\\\"");
  private static final String[] EMPTY_STR_ARRAY = new String[0];

  private TextUtils() {}

  // Delimited --

  /**
   * @param delimited line of delimited text
   * @param delimiter delimiter to split fields on
   * @return delimited strings, parsed according to RFC 4180 but with the given delimiter
   */
  public static String[] parseDelimited(String delimited, char delimiter) {
    return doParseDelimited(delimited, formatForDelimiter(delimiter));
  }

  /**
   * @param delimited PMML-style space-delimited value string
   * @return delimited values, parsed according to PMML rules
   */
  public static String[] parsePMMLDelimited(String delimited) {
    // Although you'd think ignoreSurroundingSpaces helps here, won't work with space
    // delimiter. So manually trim below.
    String[] rawResult = doParseDelimited(delimited, formatForDelimiter(' '));
    List<String> resultList = new ArrayList<>();
    for (String raw : rawResult) {
      if (!raw.isEmpty()) {
        resultList.add(raw);
      }
    }
    return resultList.toArray(EMPTY_STR_ARRAY);
  }

  private static String[] doParseDelimited(String delimited, CSVFormat format) {
    try (CSVParser parser = CSVParser.parse(delimited, format)) {
      Iterator<CSVRecord> records = parser.iterator();
      return records.hasNext() ?
          StreamSupport.stream(records.next().spliterator(), false).toArray(String[]::new) :
          EMPTY_STRING;
    } catch (IOException e) {
      throw new IllegalStateException(e); // Can't happen
    }
  }

  /**
   * @param elements values to join by the delimiter to make one line of text
   * @param delimiter delimiter to put between fields
   * @return one line of text, with RFC 4180 escaping (values with comma are quoted; double-quotes
   *  are escaped by doubling) and using the given delimiter
   */
  public static String joinDelimited(Iterable<?> elements, char delimiter) {
    return doJoinDelimited(elements, formatForDelimiter(delimiter));
  }

  /**
   * @param elements values to join by space to make one line of text
   * @return one line of text, formatted according to PMML quoting rules
   *  (\" instead of "" for escaping quotes; ignore space surrounding values
   */
  public static String joinPMMLDelimited(Iterable<?> elements) {
    String rawResult = doJoinDelimited(elements, formatForDelimiter(' '));
    // Must change "" into \"
    return TWO_DOUBLE_QUOTE_ESC.matcher(rawResult).replaceAll(SLASH_QUOTE_ESC);
  }

  /**
   * @param elements numbers to join by space to make one line of text
   * @return one line of text, formatted according to PMML quoting rules
   */
  public static String joinPMMLDelimitedNumbers(Iterable<? extends Number> elements) {
    // bit of a workaround because NON_NUMERIC quote mode still quote "-1"!
    CSVFormat format = formatForDelimiter(' ').withQuoteMode(QuoteMode.NONE);
    // No quoting, no need to convert quoting
    return doJoinDelimited(elements, format);
  }

  private static CSVFormat formatForDelimiter(char delimiter) {
    CSVFormat format = CSV_FORMAT;
    if (delimiter != format.getDelimiter()) {
      format = format.withDelimiter(delimiter);
    }
    return format;
  }

  private static String doJoinDelimited(Iterable<?> elements, CSVFormat format) {
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

  /// JSON --

  /**
   * @param json line of JSON text
   * @return delimited strings, the elements of the JSON array
   * @throws IOException if JSON parsing fails
   */
  public static String[] parseJSONArray(String json) throws IOException {
    return MAPPER.readValue(json, String[].class);
  }

  /**
   * @param elements elements to be joined in a JSON string. May be any objects.
   * @return JSON representation of the list of objects
   */
  public static String joinJSON(Iterable<?> elements) {
    try {
      return MAPPER.writeValueAsString(elements);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * @param json JSON string
   * @param clazz Java type to interpret as
   * @param <T> type that should be parsed from JSON and returned
   * @return the JSON string, parsed into the given type
   */
  public static <T> T readJSON(String json, Class<T> clazz) {
    try {
      return MAPPER.readValue(json, clazz);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * @param value value to convert
   * @param clazz desired type to interpret as
   * @param <T> type that should be parsed from JSON and returned
   * @return the given value, reinterpreted as the given type, as if serialized/deserialized
   *  via JSON to perform the conversion
   */
  public static <T> T convertViaJSON(Object value, Class<T> clazz) {
    return MAPPER.convertValue(value, clazz);
  }

}
