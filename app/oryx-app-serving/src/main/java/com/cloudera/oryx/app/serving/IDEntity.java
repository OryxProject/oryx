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

package com.cloudera.oryx.app.serving;

import java.io.Serializable;

import com.cloudera.oryx.api.serving.HasCSV;

/**
 * Superclass of objects that encapsulate, among other things, a String ID.
 */
abstract class IDEntity implements HasCSV, Serializable {

  private final String id;

  IDEntity(String id) {
    this.id = id;
  }

  public final String getID() {
    return id;
  }

  abstract String valueString();

  @Override
  public final String toString() {
    return id + ":" + valueString();
  }

  @Override
  public final String toCSV() {
    return id + "," + valueString();
  }

}
