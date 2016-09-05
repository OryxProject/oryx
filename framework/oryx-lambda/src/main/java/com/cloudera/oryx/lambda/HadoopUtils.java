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

import java.io.Closeable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.OryxShutdownHook;

/**
 * Hadoop-related utility methods.
 */
public final class HadoopUtils {

  private static final Logger log = LoggerFactory.getLogger(HadoopUtils.class);

  private static final OryxShutdownHook SHUTDOWN_HOOK = new OryxShutdownHook();

  private HadoopUtils() {}

  /**
   * Adds a shutdown hook that tries to call {@link Closeable#close()} on the given argument
   * at JVM shutdown. This integrates with Hadoop's {@link ShutdownHookManager} in order to
   * better interact with Spark's usage of the same.
   *
   * @param closeable thing to close
   */
  public static void closeAtShutdown(Closeable closeable) {
    if (SHUTDOWN_HOOK.addCloseable(closeable)) {
      try {
        // Spark uses SHUTDOWN_HOOK_PRIORITY + 30; this tries to execute earlier
        ShutdownHookManager.get().addShutdownHook(SHUTDOWN_HOOK, FileSystem.SHUTDOWN_HOOK_PRIORITY + 40);
      } catch (IllegalStateException ise) {
        log.warn("Can't close {} at shutdown since shutdown is in progress", closeable);
      }
    }
  }

}
