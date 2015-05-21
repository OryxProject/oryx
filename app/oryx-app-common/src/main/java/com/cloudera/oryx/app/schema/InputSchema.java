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

package com.cloudera.oryx.app.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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
  private final int targetFeatureIndex;
  /** Map from index among all features, to index among predictor features only. */
  private final BiMap<Integer,Integer> allToPredictorMap;

  public InputSchema(Config config) {
    List<String> givenFeatureNames = config.getStringList("oryx.input-schema.feature-names");
    if (givenFeatureNames.isEmpty()) {
      int numFeatures = config.getInt("oryx.input-schema.num-features");
      Preconditions.checkArgument(numFeatures > 0, "Neither feature-names nor num-features is set");
      List<String> generatedFeatureNames = new ArrayList<>(numFeatures);
      for (int i = 0; i < numFeatures; i++) {
        generatedFeatureNames.add(Integer.toString(i));
      }
      featureNames = generatedFeatureNames;
    } else {
      featureNames = ImmutableList.copyOf(givenFeatureNames);
    }

    int numFeatures = featureNames.size();
    Preconditions.checkArgument(new HashSet<>(featureNames).size() == numFeatures,
                                "Feature names must be unique: %s", featureNames);

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
      Preconditions.checkNotNull(givenCategoricalFeatures,
                                 "Neither numeric-features nor categorical-features was set");
      categoricalFeatures = ImmutableSet.copyOf(givenCategoricalFeatures);
      Preconditions.checkArgument(activeFeatures.containsAll(categoricalFeatures),
                                  "Active features %s not contained in categorical features %s",
                                  activeFeatures, categoricalFeatures);
      activeFeatureSet.removeAll(categoricalFeatures);
      numericFeatures = ImmutableSet.copyOf(activeFeatureSet);
    } else {
      numericFeatures = ImmutableSet.copyOf(givenNumericFeatures);
      Preconditions.checkArgument(activeFeatures.containsAll(numericFeatures),
                                  "Active features %s not contained in numeric features %s",
                                  activeFeatures, numericFeatures);
      activeFeatureSet.removeAll(numericFeatures);
      categoricalFeatures = ImmutableSet.copyOf(activeFeatureSet);
    }

    targetFeature = ConfigUtils.getOptionalString(config, "oryx.input-schema.target-feature");
    if (targetFeature != null) {
      Preconditions.checkArgument(activeFeatures.contains(targetFeature),
                                  "Target feature is not known, an ID, or ignored: %s",
                                  targetFeature);
    }
    targetFeatureIndex = targetFeature == null ? -1 : featureNames.indexOf(targetFeature);

    allToPredictorMap = HashBiMap.create();
    for (int featureIndex = 0, predictorIndex = 0;
         featureIndex < featureNames.size();
         featureIndex++) {
      if (isActive(featureIndex) && !isTarget(featureIndex)) {
        allToPredictorMap.put(featureIndex, predictorIndex);
        predictorIndex++;
      }
    }
  }

  /**
   * @return names of features in the schema, in order
   */
  public List<String> getFeatureNames() {
    return featureNames;
  }

  /**
   * @return total number of features described in the schema
   */
  public int getNumFeatures() {
    return featureNames.size();
  }

  /**
   * @return number of features that are predictors -- not an ID, not ignored, not the target
   */
  public int getNumPredictors() {
    return activeFeatures.size() - (hasTarget() ? 1 : 0);
  }

  /**
   * @param featureName feature name
   * @return {@code true} iff feature is an ID feature
   */
  public boolean isID(String featureName) {
    return idFeatures.contains(featureName);
  }

  /**
   * @param featureIndex feature index
   * @return {@code true} iff feature is an ID feature
   */
  public boolean isID(int featureIndex) {
    return isID(featureNames.get(featureIndex));
  }

  /**
   * @param featureName feature name
   * @return {@code true} iff feature is active -- not an ID or ignored
   */
  public boolean isActive(String featureName) {
    return activeFeatures.contains(featureName);
  }

  /**
   * @param featureIndex feature index
   * @return {@code true} iff feature is active -- not an ID or ignored
   */
  public boolean isActive(int featureIndex) {
    return isActive(featureNames.get(featureIndex));
  }

  /**
   * @param featureName feature name
   * @return {@code true} iff feature is numeric
   */
  public boolean isNumeric(String featureName) {
    return numericFeatures.contains(featureName);
  }

  /**
   * @param featureIndex feature index
   * @return {@code true} iff feature is a numeric
   */
  public boolean isNumeric(int featureIndex) {
    return isNumeric(featureNames.get(featureIndex));
  }

  /**
   * @param featureName feature name
   * @return {@code true} iff feature is categorical
   */
  public boolean isCategorical(String featureName) {
    return categoricalFeatures.contains(featureName);
  }

  /**
   * @param featureIndex feature index
   * @return {@code true} iff feature is a categorical
   */
  public boolean isCategorical(int featureIndex) {
    return isCategorical(featureNames.get(featureIndex));
  }

  /**
   * @param featureName feature name
   * @return {@code true} iff the feature is the target
   */
  public boolean isTarget(String featureName) {
    return featureName.equals(targetFeature);
  }

  /**
   * @param featureIndex feature index
   * @return {@code true} iff the feature is the target
   */
  public boolean isTarget(int featureIndex) {
    return targetFeatureIndex == featureIndex;
  }

  /**
   * @return name of feature that is the target
   * @throws IllegalStateException if there is no target
   */
  public String getTargetFeature() {
    Preconditions.checkState(targetFeature != null);
    return targetFeature;
  }

  /**
   * @return index of feature that is the target
   * @throws IllegalStateException if there is no target
   */
  public int getTargetFeatureIndex() {
    Preconditions.checkState(targetFeatureIndex >= 0);
    return targetFeatureIndex;
  }

  /**
   * @return {@code true} iff schema defines a target feature
   */
  public boolean hasTarget() {
    return targetFeature != null;
  }

  /**
   * @return {@code true} iff the target feature is categorical
   * @throws IllegalStateException if there is no target
   */
  public boolean isClassification() {
    return isCategorical(getTargetFeature());
  }

  /**
   * @param featureIndex index (0-based) among all features
   * @return index (0-based) among only predictors (not ID, not ignored, not target)
   */
  public int featureToPredictorIndex(int featureIndex) {
    Integer predictorIndex = allToPredictorMap.get(featureIndex);
    Preconditions.checkArgument(predictorIndex != null,
                                "No predictor for feature %s", featureIndex);
    return predictorIndex;
  }

  /**
   * @param predictorIndex index (0-based) among only predictors (not ID, not ignored, not target)
   * @return index (0-based) among all features
   */
  public int predictorToFeatureIndex(int predictorIndex) {
    Integer featureIndex = allToPredictorMap.inverse().get(predictorIndex);
    Preconditions.checkArgument(featureIndex != null,
                                "No feature for predictor %s", predictorIndex);
    return featureIndex;
  }

  @Override
  public String toString() {
    return "InputSchema[featureNames:" + featureNames + "...]";
  }

}
