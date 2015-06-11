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

package com.cloudera.oryx.app.batch.mllib.rdf;

import com.cloudera.oryx.app.batch.mllib.AbstractAppMLlibIT;

public abstract class AbstractRDFIT extends AbstractAppMLlibIT {

  protected static final int MAX_DEPTH = 8;
  protected static final int MAX_SPLIT_CANDIDATES = 100;
  protected static final String IMPURITY = "entropy";

}
