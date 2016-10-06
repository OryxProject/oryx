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

package com.cloudera.oryx.lambda;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function that deletes old data, if applicable, at each batch interval.
 *
 * @param <T> unused
 */
public final class DeleteOldDataFn<T> implements VoidFunction<T> {

  private static final Logger log = LoggerFactory.getLogger(DeleteOldDataFn.class);

  private final Configuration hadoopConf;
  private final String dataDirString;
  private final Pattern dirTimestampPattern;
  private final int maxAgeHours;

  public DeleteOldDataFn(Configuration hadoopConf,
                         String dataDirString,
                         Pattern dirTimestampPattern,
                         int maxAgeHours) {
    this.hadoopConf = hadoopConf;
    this.dataDirString = dataDirString;
    this.dirTimestampPattern = dirTimestampPattern;
    this.maxAgeHours = maxAgeHours;
  }

  @Override
  public void call(T ignored) throws IOException {
    Path dataDirPath = new Path(dataDirString + "/*");
    FileSystem fs = FileSystem.get(dataDirPath.toUri(), hadoopConf);
    FileStatus[] inputPathStatuses = fs.globStatus(dataDirPath);
    if (inputPathStatuses != null) {
      long oldestTimeAllowed =
          System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(maxAgeHours, TimeUnit.HOURS);
      Arrays.stream(inputPathStatuses).filter(FileStatus::isDirectory).map(FileStatus::getPath).
          filter(subdir -> {
            Matcher m = dirTimestampPattern.matcher(subdir.getName());
            return m.find() && Long.parseLong(m.group(1)) < oldestTimeAllowed;
          }).forEach(subdir -> {
            log.info("Deleting old data at {}", subdir);
            try {
              fs.delete(subdir, true);
            } catch (IOException e) {
              log.warn("Unable to delete {}; continuing", subdir, e);
            }
          });
    }
  }

}
