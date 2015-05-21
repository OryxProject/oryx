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

package com.cloudera.oryx.app.serving.kmeans;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.Arrays;
import java.util.List;

import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.kmeans.model.KMeansServingModel;
import com.cloudera.oryx.app.serving.kmeans.model.TestKMeansModelFactory;
import com.cloudera.oryx.lambda.serving.AbstractServingTest;
import com.cloudera.oryx.lambda.serving.MockTopicProducer;

public abstract class AbstractKMeansServingTest extends AbstractServingTest {

  @Override
  protected final List<String> getResourcePackages() {
    return Arrays.asList("com.cloudera.oryx.app.serving", "com.cloudera.oryx.app.serving.kmeans");
  }

  @Override
  protected Class<? extends ServletContextListener> getInitListenerClass() {
    return MockManagerInitListener.class;
  }

  public static class MockManagerInitListener extends AbstractServletContextListener {
    @Override
    public final void contextInitialized(ServletContextEvent sce) {
      ServletContext context = sce.getServletContext();
      context.setAttribute(AbstractOryxResource.MODEL_MANAGER_KEY, new MockServingModelManager());
      context.setAttribute(AbstractOryxResource.INPUT_PRODUCER_KEY, new MockTopicProducer());
    }
  }

  protected static class MockServingModelManager extends AbstractMockServingModelManager {
    @Override
    public KMeansServingModel getModel() {
      return TestKMeansModelFactory.buildTestModel();
    }
  }

}
