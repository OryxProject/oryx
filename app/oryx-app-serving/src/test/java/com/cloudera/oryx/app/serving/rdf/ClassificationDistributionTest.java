/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.rdf;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.MediaType;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.rdf.model.RDFServingModel;
import com.cloudera.oryx.app.serving.rdf.model.TestRDFClassificationModelFactory;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.lambda.serving.MockTopicProducer;

public final class ClassificationDistributionTest extends AbstractRDFServingTest {

  @Test
  public void testDistribution() {
    List<IDValue> recs = target("/classificationDistribution/B,0,").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals("X", recs.get(0).getID());
    Assert.assertEquals((10.0 / 90.0 + 2 * (1000.0 / 111000.0)) / 3,
                        recs.get(0).getValue(),
                        OryxTest.DOUBLE_EPSILON);
    Assert.assertEquals("Y", recs.get(1).getID());
    Assert.assertEquals((30.0 / 90.0 + 2 * (10000.0 / 111000.0)) / 3,
                        recs.get(1).getValue(),
                        OryxTest.DOUBLE_EPSILON);
    Assert.assertEquals("Z", recs.get(2).getID());
    Assert.assertEquals((50.0 / 90.0 + 2 * (100000.0 / 111000.0)) / 3,
                        recs.get(2).getValue(),
                        OryxTest.DOUBLE_EPSILON);
  }

  @Test
  public void testDistribution2() {
    List<IDValue> recs = target("/classificationDistribution/A,-5,").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals("X", recs.get(0).getID());
    Assert.assertEquals((1.0 / 6.0 + 2 * (100.0 / 1400.0)) / 3,
                        recs.get(0).getValue(),
                        OryxTest.DOUBLE_EPSILON);
    Assert.assertEquals("Y", recs.get(1).getID());
    Assert.assertEquals((2.0 / 6.0 + 2 * (400.0 / 1400.0)) / 3,
                        recs.get(1).getValue(),
                        OryxTest.DOUBLE_EPSILON);
    Assert.assertEquals("Z", recs.get(2).getID());
    Assert.assertEquals((3.0 / 6.0 + 2 * (900.0 / 1400.0)) / 3,
                        recs.get(2).getValue(),
                        OryxTest.DOUBLE_EPSILON);
  }

  @Override
  protected Class<? extends ServletContextListener> getInitListenerClass() {
    return MockClassificationManagerInitListener.class;
  }

  public static class MockClassificationManagerInitListener extends AbstractServletContextListener {
    @Override
    public final void contextInitialized(ServletContextEvent sce) {
      ServletContext context = sce.getServletContext();
      context.setAttribute(AbstractOryxResource.MODEL_MANAGER_KEY,
                           new MockClassificationServingModelManager());
      context.setAttribute(AbstractOryxResource.INPUT_PRODUCER_KEY, new MockTopicProducer());
    }
  }

  protected static class MockClassificationServingModelManager
      extends AbstractMockServingModelManager {
    @Override
    public RDFServingModel getModel() {
      return TestRDFClassificationModelFactory.buildTestModel();
    }
  }

}
