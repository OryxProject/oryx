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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;

import com.google.common.base.Preconditions;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.TestProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;

public abstract class AbstractServingTest extends JerseyTest {

  protected static final GenericType<Collection<String>> COLLECTION_STRING_TYPE =
      new GenericType<Collection<String>>() {};
  protected static final GenericType<List<Double>> LIST_DOUBLE_TYPE =
      new GenericType<List<Double>>() {};

  @Override
  public final TestContainerFactory getTestContainerFactory() {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected final DeploymentContext configureDeployment() {
    return ServletDeploymentContext.builder(configure())
        .initParam("jersey.config.server.provider.packages", getResourceClass().getPackage().getName())
        .addListener(getInitListenerClass())
        .build();
  }

  @Override
  protected final Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new ResourceConfig(getResourceClass());
  }

  protected Class<?> getResourceClass() {
    // By default, guess resource class from test class name
    String testClassName = this.getClass().getName();
    Preconditions.checkState(testClassName.endsWith("Test"));
    String resourceClassName = testClassName.substring(0, testClassName.length() - 4);
    return ClassUtils.loadClass(resourceClassName);
  }

  protected abstract Class<? extends ServletContextListener> getInitListenerClass();

}