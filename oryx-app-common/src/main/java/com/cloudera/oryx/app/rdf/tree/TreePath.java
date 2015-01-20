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

package com.cloudera.oryx.app.rdf.tree;

import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Longs;

/**
 * Encapsulates a path from root to a node in the tree.
 */
final class TreePath implements Comparable<TreePath>, Serializable {
  
  static final TreePath EMPTY = new TreePath(0L, 0);
  
  private final long leftRight;
  private final int pathLength;
  
  private TreePath(long leftRight, int pathLength) {
    Preconditions.checkArgument(pathLength >= 0 && pathLength <= 64);
    this.leftRight = leftRight;
    this.pathLength = pathLength;
  }
  
  int length() {
    return pathLength;
  }

  boolean isLeftAt(int index) {
    Preconditions.checkElementIndex(index, pathLength);
    return ((Long.MIN_VALUE >>> index) & leftRight) == 0;
  }

  TreePath extendLeft() {    
    return new TreePath(leftRight, pathLength + 1);
  }
  
  TreePath extendRight() {
    return new TreePath(leftRight | (Long.MIN_VALUE >>> pathLength), pathLength + 1);
  }
  
  @Override
  public int hashCode() {
    return Longs.hashCode(leftRight) ^ pathLength;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TreePath)) {
      return false;
    }
    TreePath other = (TreePath) o;
    return this.pathLength == other.pathLength && this.leftRight == other.leftRight;
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder(pathLength);
    for (int i = 0; i < pathLength; i++) {
      result.append(isLeftAt(i) ? '0' : '1');
    }
    return result.toString();
  }

  @Override
  public int compareTo(TreePath o) {
    int maxLength = Math.max(pathLength, o.length());
    for (int i = 0; i < maxLength; i++) {
      if (i < pathLength) {
        boolean thisLeft = isLeftAt(i);
        if (i < o.length()) {
          boolean thatLeft = o.isLeftAt(i);
          if (thisLeft != thatLeft) {
            return Booleans.compare(thatLeft, thisLeft);
          }
          // else continue
        } else {
          return thisLeft ? -1 : 1;
        }
      } else {
         // i < o.pathLength
        boolean thatLeft = o.isLeftAt(i);
        return thatLeft ? 1 : -1;
      }
    }
    return 0;
  }

}
