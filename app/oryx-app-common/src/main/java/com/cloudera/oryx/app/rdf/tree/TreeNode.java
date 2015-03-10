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

/**
 * Implementations of this interface represent nodes in a {@link DecisionTree}, whether
 * leaves ({@link TerminalNode}) or internal nodes ({@link DecisionNode}).
 *
 * @see TerminalNode
 * @see DecisionNode
 */
public abstract class TreeNode implements Serializable {

  private final String id;

  protected TreeNode(String id) {
    Preconditions.checkNotNull(id);
    this.id = id;
  }

  /**
   * @return unique ID for this node (unique within its tree)
   */
  public String getID() {
    return id;
  }

  /**
   * @return true iff the node is a leaf ({@link TerminalNode}) rather than a {@link DecisionNode}
   */
  public abstract boolean isTerminal();

}
