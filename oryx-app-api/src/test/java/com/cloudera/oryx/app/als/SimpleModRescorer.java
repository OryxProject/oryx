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

package com.cloudera.oryx.app.als;

final class SimpleModRescorer implements Rescorer, PairRescorer {
  
  private final int modulus;
  
  SimpleModRescorer(int modulus) {
    this.modulus = modulus;
  }

  @Override
  public double rescore(String itemID, double value) {
    return isFiltered(itemID) ? Double.NaN : value;
  }

  @Override
  public boolean isFiltered(String itemID) {
    return itemID.length() % modulus != 0;
  }

  @Override
  public double rescore(String fromID, String toID, double value) {
    return isFiltered(fromID, toID) ? Double.NaN : value;
  }

  @Override
  public boolean isFiltered(String fromID, String toID) {
    return fromID.length() % modulus != 0 || toID.length() % modulus != 0;
  }

}
