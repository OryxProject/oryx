package com.cloudera.oryx.ml.als.speed;

import java.io.Serializable;

final class UserItemStrength implements Serializable {

  private final int user;
  private final int item;
  private final float strength;

  UserItemStrength(int user, int item, float strength) {
    this.user = user;
    this.item = item;
    this.strength = strength;
  }

  public int getUser() {
    return user;
  }

  public int getItem() {
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
