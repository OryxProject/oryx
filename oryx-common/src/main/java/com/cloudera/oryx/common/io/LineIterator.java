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

package com.cloudera.oryx.common.io;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator over the lines of a file.
 */
final class LineIterator extends AbstractIterator<String> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(LineIterator.class);

  private final Path file;
  private BufferedReader in;
  private boolean closed;

  LineIterator(Path file) {
    this.file = file;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          log.warn("Exception while closing reader", e);
        }
      }
    }
  }

  private void maybeOpenReader() throws IOException {
    if (in == null) {
      in = new BufferedReader(new InputStreamReader(
          IOUtils.readMaybeCompressed(file), StandardCharsets.UTF_8));
    }
  }

  @Override
  protected String computeNext() {
    try {
      maybeOpenReader();
      String line = in.readLine();
      if (line == null) {
        endOfData();
        close();
      }
      return line;
    } catch (IOException e) {
      endOfData();
      close();
      throw new IllegalStateException(e);
    }
  }

}
