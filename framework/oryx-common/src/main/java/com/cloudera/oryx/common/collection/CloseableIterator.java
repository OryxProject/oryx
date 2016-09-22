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

package com.cloudera.oryx.common.collection;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Implementations are {@link Iterator}s that are also {@link Closeable}.
 *
 * @param <T> iterator element type
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {
  // Adds nothing, except no IOException from close()
  @Override
  void close();
}
