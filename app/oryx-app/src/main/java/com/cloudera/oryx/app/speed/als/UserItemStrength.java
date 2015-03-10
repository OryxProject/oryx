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

package com.cloudera.oryx.app.speed.als;

import java.io.Serializable;

/**
 * Represents a user ID, item ID and a strength of interaction between them.
 */
final class UserItemStrength implements Serializable {

  private final String user;
  private final String item;
  private final float strength;

  UserItemStrength(String user, String item, float strength) {
    this.user = user;
    this.item = item;
    this.strength = strength;
  }

  public String getUser() {
    return user;
  }

  public String getItem() {
    return item;
  }

  public float getStrength() {
    return strength;
  }

  @Override
  public String toString() {
    return user + "," + item + "," + strength;
  }

}
