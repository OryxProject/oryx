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

import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.app.classreg.predict.Prediction;
import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.common.collection.Pair;

/**
 * A decision-tree classifier. Given a set of training {@link Example}s, builds a model by randomly choosing
 * subsets of features and the training set, and then finding a binary
 * {@link com.cloudera.oryx.app.rdf.decision.Decision} over those features and data
 * that produces the largest information gain in the two subsets it implies. This is repeated to build a tree
 * of {@link DecisionNode}s. At the bottom, leaf nodes are formed ({@link TerminalNode}) that contain a
 * {@link Prediction} of the target value.
 *
 * @see DecisionForest
 */
public final class DecisionTree implements TreeBasedClassifier {

  private final TreeNode root;

  public DecisionTree(TreeNode root) {
    Objects.requireNonNull(root);
    this.root = root;
  }

  @Override
  public Prediction predict(Example test) {
    TerminalNode terminalNode = findTerminal(test);
    return terminalNode.getPrediction();
  }

  public TerminalNode findTerminal(Example example) {
    TreeNode node = root;
    while (!node.isTerminal()) {
      DecisionNode decisionNode = (DecisionNode) node;
      if (decisionNode.getDecision().isPositive(example)) {
        node = decisionNode.getRight();
      } else {
        node = decisionNode.getLeft();
      }
    }
    return (TerminalNode) node;
  }

  public TreeNode findByID(String id) {
    TreeNode node = root;
    while (!id.equals(node.getID())) {
      if (node.isTerminal()) {
        throw new IllegalArgumentException("No node with ID " + id);
      }
      Preconditions.checkState(id.startsWith(node.getID()),
                               "Node ID %s is not a prefix of %s", node.getID(), id);
      DecisionNode decisionNode = (DecisionNode) node;
      char decisionChar = id.charAt(node.getID().length());
      Preconditions.checkState(decisionChar == '-' || decisionChar == '+');
      if (decisionChar == '+') {
        node = decisionNode.getRight();
      } else {
        node = decisionNode.getLeft();
      }
    }
    return node;
  }

  @Override
  public void update(Example train) {
    TerminalNode terminalNode = findTerminal(train);
    terminalNode.update(train);
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (root != null) {
      Deque<Pair<TreeNode,TreePath>> toPrint = new LinkedList<>();
      toPrint.push(new Pair<>(root, TreePath.EMPTY));
      while (!toPrint.isEmpty()) {
        Pair<TreeNode,TreePath> entry = toPrint.pop();
        TreeNode node = entry.getFirst();
        TreePath path = entry.getSecond();
        int pathLength = path.length();
        for (int i = 0; i < pathLength; i++) {
          if (i == pathLength - 1) {
            result.append(" +-");
          } else {
            result.append(path.isLeftAt(i) ? " | " : "   ");
          }
        }
        result.append(node).append('\n');
        if (node != null && !node.isTerminal()) {
          DecisionNode decisionNode = (DecisionNode) node;
          toPrint.push(new Pair<>(decisionNode.getRight(), path.extendRight()));
          toPrint.push(new Pair<>(decisionNode.getLeft(), path.extendLeft()));
        }
      }
    }
    return result.toString();
  }

}
