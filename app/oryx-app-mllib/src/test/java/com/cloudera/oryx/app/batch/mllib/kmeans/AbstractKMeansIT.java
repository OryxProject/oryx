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

package com.cloudera.oryx.app.batch.mllib.kmeans;

import com.cloudera.oryx.app.batch.mllib.AbstractAppMLlibIT;

public abstract class AbstractKMeansIT extends AbstractAppMLlibIT {

  protected static final int NUM_CLUSTERS = 3;
  protected static final int NUM_FEATURES = 5;
  protected static final KMeansEvalStrategy EVALUATION_STRATEGY = KMeansEvalStrategy.SSE;

}
