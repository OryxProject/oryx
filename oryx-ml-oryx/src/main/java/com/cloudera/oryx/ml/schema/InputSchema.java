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

package com.cloudera.oryx.ml.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Represents the essential schema information needed for some learning problems.
 */
public final class InputSchema implements Serializable {

  private final List<String> featureNames;
  private final Collection<String> idFeatures;
  private final Collection<String> activeFeatures;
  private final Collection<String> numericFeatures;
  private final Collection<String> categoricalFeatures;
  private final String targetFeature;

  public InputSchema(Config config) {
    List<String> givenFeatureNames = config.getStringList("oryx.input-schema.feature-names");
    if (givenFeatureNames.isEmpty()) {
      int numFeatures = config.getInt("oryx.input-schema.num-features");
      Preconditions.checkArgument(numFeatures > 0);
      List<String> generatedFeatureNames = new ArrayList<>(numFeatures);
      for (int i = 0; i < numFeatures; i++) {
        generatedFeatureNames.add(Integer.toString(i));
      }
      featureNames = generatedFeatureNames;
    } else {
      featureNames = ImmutableList.copyOf(givenFeatureNames);
    }

    idFeatures = ImmutableSet.copyOf(config.getStringList("oryx.input-schema.id-features"));
    Preconditions.checkArgument(featureNames.containsAll(idFeatures));

    Collection<String> ignoredFeatures =
        ImmutableSet.copyOf(config.getStringList("oryx.input-schema.ignored-features"));
    Preconditions.checkArgument(featureNames.containsAll(ignoredFeatures));

    Collection<String> activeFeatureSet = new HashSet<>(featureNames);
    activeFeatureSet.removeAll(idFeatures);
    activeFeatureSet.removeAll(ignoredFeatures);
    activeFeatures = ImmutableSet.copyOf(activeFeatureSet);

    List<String> givenNumericFeatures =
        ConfigUtils.getOptionalStringList(config, "oryx.input-schema.numeric-features");
    List<String> givenCategoricalFeatures =
        ConfigUtils.getOptionalStringList(config, "oryx.input-schema.categorical-features");

    if (givenNumericFeatures == null) {
      Preconditions.checkNotNull(givenCategoricalFeatures);
      categoricalFeatures = ImmutableSet.copyOf(givenCategoricalFeatures);
      Preconditions.checkArgument(activeFeatures.containsAll(categoricalFeatures));
      activeFeatureSet.removeAll(categoricalFeatures);
      numericFeatures = ImmutableSet.copyOf(activeFeatureSet);
    } else {
      numericFeatures = ImmutableSet.copyOf(givenNumericFeatures);
      Preconditions.checkArgument(activeFeatures.containsAll(numericFeatures));
      activeFeatureSet.removeAll(numericFeatures);
      categoricalFeatures = ImmutableSet.copyOf(activeFeatureSet);
    }

    targetFeature = ConfigUtils.getOptionalString(config, "oryx.input-schema.target-feature");
  }

  public List<String> getFeatureNames() {
    return featureNames;
  }

  public int getNumFeatures() {
    return featureNames.size();
  }

  public boolean isID(String featureName) {
    return idFeatures.contains(featureName);
  }

  public boolean isID(int featureIndex) {
    return isID(featureNames.get(featureIndex));
  }

  public boolean isActive(String featureName) {
    return activeFeatures.contains(featureName);
  }

  public boolean isActive(int featureIndex) {
    return isActive(featureNames.get(featureIndex));
  }

  public boolean isNumeric(String featureName) {
    return numericFeatures.contains(featureName);
  }

  public boolean isNumeric(int featureIndex) {
    return isNumeric(featureNames.get(featureIndex));
  }

  public boolean isCategorical(String featureName) {
    return categoricalFeatures.contains(featureName);
  }

  public boolean isCategorical(int featureIndex) {
    return isCategorical(featureNames.get(featureIndex));
  }

  public boolean isTarget(String featureName) {
    return targetFeature != null && targetFeature.equals(featureName);
  }

  public String getTargetFeature() {
    return targetFeature;
  }

  public boolean isTarget(int featureIndex) {
    return isTarget(featureNames.get(featureIndex));
  }

  public boolean isClassification() {
    return isCategorical(featureNames.indexOf(targetFeature));
  }

  @Override
  public String toString() {
    return "InputSchema[featureNames:" + featureNames + "...]";
  }

}
