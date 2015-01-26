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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract implementation of {@link RescorerProvider} which implements all methods to
 * return {@code null}.
 */
public abstract class AbstractRescorerProvider implements RescorerProvider {

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getRecommendRescorer(List<String> userIDs, List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getRecommendToAnonymousRescorer(List<String> itemIDs, List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public Rescorer getMostPopularItemsRescorer(List<String> args) {
    return null;
  }

  /**
   * @return {@code null}
   */
  @Override
  public PairRescorer getMostSimilarItemsRescorer(List<String> args) {
    return null;
  }

  /**
   * @param classNamesString a comma-delimited list of class names, where classes implement
   *  {@link RescorerProvider}
   * @return a {@link RescorerProvider} which rescores using all of them
   */
  public static RescorerProvider loadRescorerProviders(String classNamesString) {
    if (classNamesString == null || classNamesString.isEmpty()) {
      return null;
    }
    String[] classNames = classNamesString.split(",");
    if (classNames.length == 1) {
      return loadInstanceOf(classNames[0]);
    }
    List<RescorerProvider> providers = new ArrayList<>(classNames.length);
    for (String className : classNames) {
      providers.add(loadInstanceOf(className));
    }
    return new MultiRescorerProvider(providers);
  }

  private static RescorerProvider loadInstanceOf(String implClassName) {
    try {
      Class<? extends RescorerProvider> configClass =
          Class.forName(implClassName).asSubclass(RescorerProvider.class);
      Constructor<? extends RescorerProvider> constructor = configClass.getConstructor();
      return constructor.newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Could not load " + implClassName + " due to exception", e);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException(
          "Could not instantiate " + implClassName + " due to exception", e);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException(
          "Could not instantiate " + implClassName + " due to exception", ite.getCause());
    }
  }

}
