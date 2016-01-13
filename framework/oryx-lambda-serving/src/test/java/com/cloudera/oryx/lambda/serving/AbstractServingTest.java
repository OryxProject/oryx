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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Assert;
import org.junit.Before;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.serving.AbstractServingModelManager;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.random.RandomManager;

public abstract class AbstractServingTest extends JerseyTest {

  protected static final float FLOAT_EPSILON = 1.0e-6f;
  protected static final double DOUBLE_EPSILON = 1.0e-12;

  protected static final GenericType<List<String>> LIST_STRING_TYPE =
      new GenericType<List<String>>() {};
  protected static final GenericType<List<Double>> LIST_DOUBLE_TYPE =
      new GenericType<List<Double>>() {};

  @Before
  public final void initRandom() {
    RandomManager.useTestSeed();
  }

  @Before
  public void clearProducerData() {
    MockTopicProducer.getData().clear();
  }

  @Override
  public final TestContainerFactory getTestContainerFactory() {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
    config.register(MultiPartFeature.class);
  }

  protected void configureProperties() {
    //enable(TestProperties.LOG_TRAFFIC);
    //enable(TestProperties.DUMP_ENTITY);
  }

  @Override
  protected final DeploymentContext configureDeployment() {
    configureProperties();
    String joinedPackages = String.join(",", getResourcePackages()) +
        ",com.cloudera.oryx.lambda.serving";
    return ServletDeploymentContext.builder(OryxApplication.class)
        .initParam("javax.ws.rs.Application", OryxApplication.class.getName())
        .contextParam(OryxApplication.class.getName() + ".packages", joinedPackages)
        .addListener(getInitListenerClass())
        .build();
  }

  protected abstract List<String> getResourcePackages();

  protected abstract Class<? extends ServletContextListener> getInitListenerClass();

  protected final Response getFormPostResponse(String data,
                                               String endpoint,
                                               Class<? extends OutputStream> compressingClass,
                                               String encoding) throws IOException {
    byte[] bytes;
    if (compressingClass == null) {
      bytes = data.getBytes(StandardCharsets.UTF_8);
    } else {
      bytes = compress(data, compressingClass);
    }
    MediaType type =
        encoding == null ? MediaType.TEXT_PLAIN_TYPE : new MediaType("application", encoding);
    InputStream in = new ByteArrayInputStream(bytes);
    StreamDataBodyPart filePart = new StreamDataBodyPart("data", in, "data", type);
    try (MultiPart multiPart = new MultiPart(MediaType.MULTIPART_FORM_DATA_TYPE)) {
      multiPart.getBodyParts().add(filePart);
      return target(endpoint).request().post(
          Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE));
    }
  }

  protected static byte[] compress(
      String data, Class<? extends OutputStream> compressingClass) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (OutputStream compressingStream =
             ClassUtils.loadInstanceOf(compressingClass.getName(),
                                       compressingClass,
                                       new Class<?>[]{OutputStream.class},
                                       new Object[]{bytes})) {
      if (compressingStream instanceof ZipOutputStream) {
        ((ZipOutputStream) compressingStream).putNextEntry(new ZipEntry("data"));
      }
      compressingStream.write(data.getBytes(StandardCharsets.UTF_8));
      if (compressingStream instanceof ZipOutputStream) {
        ((ZipOutputStream) compressingStream).closeEntry();
      }
      compressingStream.flush();
    } catch (IOException e) {
      // Can't happen
      throw new IllegalStateException(e);
    }
    return bytes.toByteArray();
  }

  protected static void checkResponse(Response response,
                                      Response.Status expectedStatus,
                                      String[][] expectedTopic) {
    Assert.assertEquals(expectedStatus.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockTopicProducer.getData();
    for (int i = 0; i < data.size(); i++) {
      Pair<String,String> actual = data.get(i);
      Assert.assertNotNull(actual.getFirst());
      String[] tokens = actual.getSecond().split(",");
      Assert.assertArrayEquals(expectedTopic[i], tokens);
    }
  }

  public abstract static class AbstractServletContextListener implements ServletContextListener {
    @Override
    public final void contextDestroyed(ServletContextEvent sce) {
      // do nothing
    }
  }

  protected abstract static class AbstractMockServingModelManager
      extends AbstractServingModelManager<String> {
    protected AbstractMockServingModelManager(Config config) {
      super(config);
    }
    @Override
    public final void consume(Iterator<KeyMessage<String, String>> updateIterator, Configuration hadoopConf) {
      throw new UnsupportedOperationException();
    }
  }

}