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

package com.cloudera.oryx.common.lang;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM-related utility methods.
 */
public final class JVMUtils {

  private static final Logger log = LoggerFactory.getLogger(JVMUtils.class);

  private static final OryxShutdownHook SHUTDOWN_HOOK = new OryxShutdownHook();

  private JVMUtils() {}

  /**
   * Adds a shutdown hook that tries to call {@link Closeable#close()} on the given argument
   * at JVM shutdown.
   *
   * @param closeable thing to close
   */
  public static void closeAtShutdown(Closeable closeable) {
    if (SHUTDOWN_HOOK.addCloseable(closeable)) {
      try {
        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK, "OryxShutdownHookThread"));
      } catch (IllegalStateException ise) {
        log.warn("Can't close {} at shutdown since shutdown is in progress", closeable);
      }
    }
  }

  /**
   * @return approximate heap used, in bytes
   */
  public static long getUsedMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

}
