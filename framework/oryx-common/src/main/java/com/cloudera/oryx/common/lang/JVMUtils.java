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
import java.util.Deque;
import java.util.LinkedList;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.io.IOUtils;

/**
 * JVM-related utility methods.
 */
public final class JVMUtils {

  private static final Deque<Closeable> closeAtShutdown = new LinkedList<>();

  private JVMUtils() {
  }

  /**
   * Adds a shutdown hook that try to call {@link Closeable#close()} on the given argument
   * at JVM shutdown.
   *
   * @param closeable thing to close
   */
  public static void closeAtShutdown(Closeable closeable) {
    Preconditions.checkNotNull(closeable);
    synchronized (closeAtShutdown) {
      if (closeAtShutdown.isEmpty()) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
          @Override
          public void run() {
            synchronized (closeAtShutdown) {
              for (Closeable c : closeAtShutdown) {
                IOUtils.closeQuietly(c);
              }
            }
          }
        }));
      }
      closeAtShutdown.push(closeable);
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
