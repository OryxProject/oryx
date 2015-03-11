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

import com.cloudera.oryx.app.rdf.decision.Decision;

/**
 * Represents an internal node in a {@link DecisionTree}, which encapsulates a binary decision over a
 * feature in an example.
 */
public final class DecisionNode extends TreeNode {

  private final Decision decision;
  private final TreeNode left;
  private final TreeNode right;

  public DecisionNode(String id, Decision decision, TreeNode left, TreeNode right) {
    super(id);
    this.decision = decision;
    this.left = left;
    this.right = right;
  }

  public Decision getDecision() {
    return decision;
  }

  @Override
  public boolean isTerminal() {
    return false;
  }

  public TreeNode getLeft() {
    return left;
  }

  public TreeNode getRight() {
    return right;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DecisionNode)) {
      return false;
    }
    DecisionNode other = (DecisionNode) o;
    return decision.equals(other.decision) && left.equals(other.left) && right.equals(other.right);
  }

  @Override
  public int hashCode() {
    return decision.hashCode() ^ left.hashCode() ^ right.hashCode();
  }

  @Override
  public String toString() {
    return decision.toString();
  }

}
