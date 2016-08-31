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

package com.cloudera.oryx.app.serving.als.model;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.serving.AbstractServingModelManager;
import com.cloudera.oryx.app.als.MultiRescorerProvider;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.lang.RateLimitCheck;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * A {@link com.cloudera.oryx.api.serving.ServingModelManager} that manages and provides access to an
 * {@link ALSServingModel} for the ALS Serving Layer application.
 */
public final class ALSServingModelManager extends AbstractServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelManager.class);

  private ALSServingModel model;
  private boolean triggeredSolver;
  private final double sampleRate;
  private final double minModelLoadFraction;
  private final RescorerProvider rescorerProvider;
  private final RateLimitCheck logRateLimit;

  public ALSServingModelManager(Config config) {
    super(config);
    String rescorerProviderClass =
        ConfigUtils.getOptionalString(config, "oryx.als.rescorer-provider-class");
    rescorerProvider = loadRescorerProviders(rescorerProviderClass);
    sampleRate = config.getDouble("oryx.als.sample-rate");
    minModelLoadFraction = config.getDouble("oryx.serving.min-model-load-fraction");
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);
    Preconditions.checkArgument(minModelLoadFraction >= 0.0 && minModelLoadFraction <= 1.0);
    logRateLimit = new RateLimitCheck(1, TimeUnit.MINUTES);
  }

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "UP":
        if (model == null) {
          return; // No model to interpret with yet, so skip it
        }
        List<?> update = TextUtils.readJSON(message, List.class);
        // Update
        String id = update.get(1).toString();
        float[] vector = TextUtils.convertViaJSON(update.get(2), float[].class);
        switch (update.get(0).toString()) {
          case "X":
            model.setUserVector(id, vector);
            if (update.size() > 3) {
              @SuppressWarnings("unchecked")
              Collection<String> knownItems = (Collection<String>) update.get(3);
              model.addKnownItems(id, knownItems);
            }
            break;
          case "Y":
            model.setItemVector(id, vector);
            // Right now, no equivalent knownUsers
            break;
          default:
            throw new IllegalArgumentException("Bad message: " + message);
        }
        if (logRateLimit.test()) {
          log.info("{}", model);
          // Arbitrarily take this opportunity to see if solver can be pre-triggered
          // to speed up first access to endpoints that need the solver
          if (!triggeredSolver && model.getFractionLoaded() >= minModelLoadFraction) {
            triggeredSolver = true;
            model.precomputeSolvers();
          }
        }
        break;

      case "MODEL":
      case "MODEL-REF":
        log.info("Loading new model");
        PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
        if (pmml == null) {
          return;
        }

        int features = Integer.parseInt(AppPMMLUtils.getExtensionValue(pmml, "features"));
        boolean implicit = Boolean.valueOf(AppPMMLUtils.getExtensionValue(pmml, "implicit"));

        if (model == null || features != model.getFeatures()) {
          log.warn("No previous model, or # features has changed; creating new one");
          model = new ALSServingModel(features, implicit, sampleRate, rescorerProvider);
        }

        log.info("Updating model");
        // Remove users/items no longer in the model
        Collection<String> XIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "XIDs"));
        Collection<String> YIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "YIDs"));
        model.retainRecentAndKnownItems(XIDs, YIDs);
        model.retainRecentAndUserIDs(XIDs);
        model.retainRecentAndItemIDs(YIDs);
        log.info("Model updated: {}", model);
        break;

      default:
        throw new IllegalArgumentException("Bad key: " + key);
    }
  }

  @Override
  public ALSServingModel getModel() {
    return model;
  }

  /**
   * @param classNamesString a comma-delimited list of class names, where classes implement
   *  {@link RescorerProvider}
   * @return a {@link RescorerProvider} which rescores using all of them
   */
  static RescorerProvider loadRescorerProviders(String classNamesString) {
    if (classNamesString == null || classNamesString.isEmpty()) {
      return null;
    }
    String[] classNames = classNamesString.split(",");
    if (classNames.length == 1) {
      return loadInstanceOf(classNames[0]);
    }
    RescorerProvider[] providers = new RescorerProvider[classNames.length];
    for (int i = 0; i < classNames.length; i++) {
      providers[i] = loadInstanceOf(classNames[i]);
    }
    return MultiRescorerProvider.of(providers);
  }

  private static RescorerProvider loadInstanceOf(String implClassName) {
    try {
      // ClassUtils is not available here
      Class<? extends RescorerProvider> configClass =
          Class.forName(implClassName, true, RescorerProvider.class.getClassLoader())
              .asSubclass(RescorerProvider.class);
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
