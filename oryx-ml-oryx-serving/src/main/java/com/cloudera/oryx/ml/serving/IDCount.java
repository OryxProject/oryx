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

package com.cloudera.oryx.ml.serving;

public final class IDCount {

  private String id;
  private int count;

  public IDCount() {
    this(null, 0);
  }

  public IDCount(String id, int count) {
    this.id = id;
    this.count = count;
  }

  public String getID() {
    return id;
  }

  public void setID(String id) {
    this.id = id;
  }

  public int getValue() {
    return count;
  }

  public void setValue(int count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return id + ":" + count;
  }

}
