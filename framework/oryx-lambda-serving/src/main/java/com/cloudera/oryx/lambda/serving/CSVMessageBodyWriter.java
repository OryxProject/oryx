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

package com.cloudera.oryx.lambda.serving;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

import com.cloudera.oryx.api.serving.HasCSV;

/**
 * A {@link MessageBodyWriter} that can produce CSV ({@code text/csv}). If given an
 * {@link Iterable}, produces a line of CSV for each element. If an element implements
 * {@link HasCSV} then {@link HasCSV#toCSV()} is used to compute its CSV representation; otherwise
 * {@link Object#toString()} is.
 */
@Produces({MediaType.TEXT_PLAIN, "text/csv"})
public final class CSVMessageBodyWriter implements MessageBodyWriter<Object> {

  private static final MediaType TEXT_CSV_TYPE = new MediaType("text", "csv");

  @Override
  public boolean isWriteable(Class<?> type,
                             Type genericType,
                             Annotation[] annotations,
                             MediaType mediaType) {
    return MediaType.TEXT_PLAIN_TYPE.equals(mediaType) || TEXT_CSV_TYPE.equals(mediaType);
  }

  @Override
  public long getSize(Object o,
                      Class<?> type,
                      Type genericType,
                      Annotation[] annotations,
                      MediaType mediaType) {
    return -1L;
  }

  @Override
  public void writeTo(Object o,
                      Class<?> type,
                      Type genericType,
                      Annotation[] annotations,
                      MediaType mediaType,
                      MultivaluedMap<String,Object> httpHeaders,
                      OutputStream entityStream) throws IOException {
    Writer out = new OutputStreamWriter(entityStream, StandardCharsets.UTF_8);
    if (Iterable.class.isAssignableFrom(type)) {
      for (Object row : (Iterable<?>) o) {
        out.append(toCSV(row)).append('\n');
      }
    } else {
      out.append(toCSV(o)).append('\n');
    }
    out.flush();
  }

  private static String toCSV(Object row) {
    if (row instanceof HasCSV) {
      return ((HasCSV) row).toCSV();
    } else {
      return row.toString();
    }
  }

}
