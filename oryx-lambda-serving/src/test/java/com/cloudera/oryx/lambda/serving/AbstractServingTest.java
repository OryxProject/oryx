/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.lambda.serving;

import java.util.Collection;
import java.util.List;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.GenericType;

import com.google.common.base.Joiner;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Before;

import com.cloudera.oryx.common.random.RandomManager;

public abstract class AbstractServingTest extends JerseyTest {

  protected static final float FLOAT_EPSILON = 1.0e-6f;
  protected static final double DOUBLE_EPSILON = 1.0e-12;

  protected static final GenericType<Collection<String>> COLLECTION_STRING_TYPE =
      new GenericType<Collection<String>>() {};
  protected static final GenericType<List<Double>> LIST_DOUBLE_TYPE =
      new GenericType<List<Double>>() {};

  @Before
  public final void initRandom() {
    RandomManager.useTestSeed();
  }

  @Override
  public final TestContainerFactory getTestContainerFactory() {
    return new GrizzlyWebTestContainerFactory();
  }

  protected void configureProperties() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
  }

  @Override
  protected final DeploymentContext configureDeployment() {
    configureProperties();
    String joinedPackages = Joiner.on(',').join(getResourcePackages());
    return ServletDeploymentContext.builder(OryxApplication.class)
        .initParam("javax.ws.rs.Application", OryxApplication.class.getName())
        .contextParam(OryxApplication.class.getName() + ".packages", joinedPackages)
        .addListener(getInitListenerClass())
        .build();
  }

  protected abstract List<String> getResourcePackages();

  protected abstract Class<? extends ServletContextListener> getInitListenerClass();

}