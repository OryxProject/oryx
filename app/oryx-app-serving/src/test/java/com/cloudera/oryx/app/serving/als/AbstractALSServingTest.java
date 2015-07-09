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

package com.cloudera.oryx.app.serving.als;

import java.util.Arrays;
import java.util.List;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;

import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.IDCount;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.app.serving.als.model.TestALSModelFactory;
import com.cloudera.oryx.lambda.serving.AbstractServingTest;
import com.cloudera.oryx.lambda.serving.MockTopicProducer;

public abstract class AbstractALSServingTest extends AbstractServingTest {

  protected static final GenericType<List<IDValue>> LIST_ID_VALUE_TYPE =
      new GenericType<List<IDValue>>() {};
  protected static final GenericType<List<IDCount>> LIST_ID_COUNT_TYPE =
      new GenericType<List<IDCount>>() {};

  @Override
  protected final List<String> getResourcePackages() {
    return Arrays.asList("com.cloudera.oryx.app.serving", "com.cloudera.oryx.app.serving.als");
  }

  @Override
  protected Class<? extends ServletContextListener> getInitListenerClass() {
    return MockManagerInitListener.class;
  }

  public static class MockManagerInitListener extends AbstractServletContextListener {
    @Override
    public final void contextInitialized(ServletContextEvent sce) {
      ServletContext context = sce.getServletContext();
      context.setAttribute(AbstractOryxResource.MODEL_MANAGER_KEY, getModelManager());
      context.setAttribute(AbstractOryxResource.INPUT_PRODUCER_KEY, new MockTopicProducer());
    }
    protected MockServingModelManager getModelManager() {
      return new MockServingModelManager();
    }
  }

  protected static class MockServingModelManager extends AbstractMockServingModelManager {
    @Override
    public ALSServingModel getModel() {
      return TestALSModelFactory.buildTestModel();
    }
  }

  protected final void testOffset(String requestPath, int howMany, int offset, int expectedSize) {
    List<?> results = target(requestPath)
        .queryParam("howMany", Integer.toString(howMany))
        .queryParam("offset", Integer.toString(offset))
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(expectedSize, results.size());
  }

  protected final void testHowMany(String requestPath, int howMany, int expectedSize) {
    List<?> results = target(requestPath)
        .queryParam("howMany", Integer.toString(howMany))
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(expectedSize, results.size());
  }

  protected static void testTopByValue(int expectedSize,
                                       List<IDValue> values,
                                       boolean reverse) {
    Assert.assertEquals(expectedSize, values.size());
    for (int i = 0; i < values.size(); i++) {
      IDValue value = values.get(i);
      double thisScore = value.getValue();
      Assert.assertFalse(Double.isNaN(thisScore));
      Assert.assertFalse(Double.isInfinite(thisScore));
      if (i > 0) {
        double lastScore = values.get(i-1).getValue();
        if (reverse) {
          Assert.assertTrue(lastScore <= thisScore);
        } else {
          Assert.assertTrue(lastScore >= thisScore);
        }
      }
    }
  }

  protected static void testTopCount(int expectedSize, List<IDCount> top) {
    Assert.assertEquals(expectedSize, top.size());
    for (int i = 0; i < top.size(); i++) {
      int thisCount = top.get(i).getCount();
      Assert.assertTrue(thisCount >= 1);
      if (i > 0) {
        Assert.assertTrue(top.get(i - 1).getCount() >= thisCount);
      }
    }
  }

  protected static void testCSVTopByScore(int expectedSize, String response) {
    testCSVTop(expectedSize, response, false, false);
  }

  protected static void testCSVLeastByScore(int expectedSize, String response) {
    testCSVTop(expectedSize, response, false, true);
  }

  protected static void testCSVTopByCount(int expectedSize, String response) {
    testCSVTop(expectedSize, response, true, false);
  }

  private static void testCSVTop(int expectedSize,
                                 String response,
                                 boolean counts,
                                 boolean reverse) {
    String[] rows = response.split("\n");
    Assert.assertEquals(expectedSize, rows.length);
    for (int i = 0; i < rows.length; i++) {
      String row = rows[i];
      String[] tokens = row.split(",");
      if (counts) {
        int count = Integer.parseInt(tokens[1]);
        Assert.assertTrue(count > 0);
      }
      double thisScore = Double.parseDouble(tokens[1]);
      Assert.assertFalse(Double.isNaN(thisScore));
      Assert.assertFalse(Double.isInfinite(thisScore));
      if (i > 0) {
        double lastScore = Double.parseDouble(rows[i-1].split(",")[1]);
        if (reverse) {
          Assert.assertTrue(lastScore <= thisScore);
        } else {
          Assert.assertTrue(lastScore >= thisScore);
        }
      }
    }
  }

  protected static void testCSVScores(int expectedSize, String response) {
    String[] rows = response.split("\n");
    Assert.assertEquals(expectedSize, rows.length);
    for (String row : rows) {
      double score = Double.parseDouble(row);
      Assert.assertFalse(Double.isNaN(score));
      Assert.assertFalse(Double.isInfinite(score));
    }
  }

}
