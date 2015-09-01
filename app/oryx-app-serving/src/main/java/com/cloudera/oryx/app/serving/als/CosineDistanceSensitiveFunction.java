/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.app.serving.als;

import java.util.function.ToDoubleFunction;

/**
 * Implementations of this type of function return a value that depends on
 * cosine distance to a target vector and thus makes sense to use with the
 * locality-sensitive hash logic in {@link com.cloudera.oryx.app.serving.als.model.ALSServingModel}.
 */
public interface CosineDistanceSensitiveFunction extends ToDoubleFunction<float[]> {

  /**
   * @return vector whose cosine distance to other vectors influence the function value
   */
  float[] getTargetVector();

}
