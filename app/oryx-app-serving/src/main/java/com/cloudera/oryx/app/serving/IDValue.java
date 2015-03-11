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

package com.cloudera.oryx.app.serving;

public final class IDValue extends IDEntity {

  private final double value;

  public IDValue() {
    this(null, 0.0);
  }

  public IDValue(String id, double value) {
    super(id);
    this.value = value;
  }

  public double getValue() {
    return value;
  }

  @Override
  public String toString() {
    return getID() + ":" + value;
  }

  @Override
  public String toCSV() {
    return getID() + "," + value;
  }

}
