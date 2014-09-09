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

package com.cloudera.oryx.ml.serving.als;

import java.util.Iterator;
import java.util.List;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.GenericType;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.Assert;

import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.serving.AbstractServingTest;
import com.cloudera.oryx.lambda.serving.ServingModelManager;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;
import com.cloudera.oryx.ml.serving.als.model.TestALSModelFactory;

public abstract class AbstractALSServingTest extends AbstractServingTest {

  protected static final GenericType<List<IDValue>> LIST_ID_VALUE_TYPE =
      new GenericType<List<IDValue>>() {};

  @Override
  protected final String getResourcePackage() {
    return getClass().getPackage().getName();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
    config.register(MultiPartFeature.class);
  }

  @Override
  protected Class<? extends ServletContextListener> getInitListenerClass() {
    return MockManagerInitListener.class;
  }

  public static class MockManagerInitListener implements ServletContextListener {
    @Override
    public final void contextInitialized(ServletContextEvent sce) {
      ServletContext context = sce.getServletContext();
      context.setAttribute(AbstractALSResource.MODEL_MANAGER_KEY, getModelManager());
      context.setAttribute(AbstractALSResource.INPUT_PRODUCER_KEY, new MockQueueProducer());
    }
    protected MockServingModelManager getModelManager() {
      return new MockServingModelManager();
    }
    @Override
    public final void contextDestroyed(ServletContextEvent sce) {
      // do nothing
    }
  }

  protected static class MockServingModelManager implements ServingModelManager<String> {
    @Override
    public final void consume(Iterator<KeyMessage<String, String>> updateIterator) {
      throw new UnsupportedOperationException();
    }
    @Override
    public ALSServingModel getModel() {
      return TestALSModelFactory.buildTestModel();
    }
    @Override
    public final void close() {
      // do nothing
    }
  }

  protected final void testOffset(String requestPath, int howMany, int offset, int expectedSize) {
    List<?> results = target(requestPath)
        .queryParam("howMany", Integer.toString(howMany))
        .queryParam("offset", Integer.toString(offset))
        .request().get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(expectedSize, results.size());
  }

  protected final void testHowMany(String requestPath, int howMany, int expectedSize) {
    List<?> results = target(requestPath)
        .queryParam("howMany",Integer.toString(howMany))
        .request().get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(expectedSize, results.size());
  }

}
