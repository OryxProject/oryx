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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import javax.servlet.ServletContextListener;
import javax.ws.rs.core.MediaType;

import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.JVMUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.app.serving.als.model.LoadTestALSModelFactory;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * <p>This can be run from the top level of the project with
 * {@code mvn -Dtest=com.cloudera.oryx.app.serving.als.LoadBenchmark \
 *  -DargLine="-Xmx... " \
 *  surefire:test -pl app/oryx-app-serving}</p>
 *
 * <p>Additional parameters defined in {@link LoadTestALSModelFactory} like
 * {@link LoadTestALSModelFactory#USERS} can be set by adding args like
 * {@code -Doryx.test.als.benchmark.users=1000000}.</p>
 *
 * <p>Note this test isn't run by default by surefire or failsafe. It is activated by a profile.</p>
 */
public final class LoadBenchmark extends AbstractALSServingTest {

  private static final Logger log = LoggerFactory.getLogger(LoadBenchmark.class);

  @Override
  protected void configureProperties() {
    super.configureProperties();
    if (isEnabled(TestProperties.LOG_TRAFFIC)) {
      disable(TestProperties.LOG_TRAFFIC);
    }
    if (isEnabled(TestProperties.DUMP_ENTITY)) {
      disable(TestProperties.DUMP_ENTITY);
    }
  }

  @Test
  public void testRecommendLoad() throws Exception {
    AtomicLong count = new AtomicLong();
    Mean meanReqTimeMS = new Mean();
    long start = System.currentTimeMillis();

    int workers = LoadTestALSModelFactory.WORKERS;
    ForkJoinPool pool = new ForkJoinPool(workers);
    try {
      pool.submit(() -> IntStream.range(0, workers).parallel().forEach(i -> {
        RandomGenerator random = RandomManager.getRandom(Integer.toString(i).hashCode() ^ System.nanoTime());
        for (int j = 0; j < LoadTestALSModelFactory.REQS_PER_WORKER; j++) {
          String userID = "U" + random.nextInt(LoadTestALSModelFactory.USERS);
          long callStart = System.currentTimeMillis();
          target("/recommend/" + userID).request()
              .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
          long timeMS = System.currentTimeMillis() - callStart;
          synchronized (meanReqTimeMS) {
            meanReqTimeMS.increment(timeMS);
          }
          long currentCount = count.incrementAndGet();
          if (currentCount % 100 == 0) {
            log(currentCount, meanReqTimeMS, start);
          }
        }
      })).get();
    } finally {
      pool.shutdown();
    }

    int totalRequests = workers * LoadTestALSModelFactory.REQS_PER_WORKER;
    log(totalRequests, meanReqTimeMS, start);
  }

  private static void log(long currentCount, Mean meanReqTimeMS, long start) {
    long used = JVMUtils.getUsedMemory() / 1_000_000;
    long requestsPerSecond =
        Math.round((1000.0 * currentCount) / (System.currentTimeMillis() - start));
    long msPerRequest;
    synchronized (meanReqTimeMS) {
      msPerRequest = Math.round(meanReqTimeMS.getResult());
    }
    log.info("{} complete\t{} req/s\t~{} ms/req\t{}MB heap",
        currentCount,
        requestsPerSecond,
        msPerRequest,
        used
    );
  }

  @Override
  protected Class<? extends ServletContextListener> getInitListenerClass() {
    return MockLoadTestManagerInitListener.class;
  }

  public static final class MockLoadTestManagerInitListener
      extends AbstractALSServingTest.MockManagerInitListener {
    @Override
    protected AbstractALSServingTest.MockServingModelManager getModelManager() {
      return new MockLoadTestServingModelManager(ConfigUtils.getDefault());
    }
  }

  private static final class MockLoadTestServingModelManager
      extends AbstractALSServingTest.MockServingModelManager {
    private final ALSServingModel model = LoadTestALSModelFactory.buildTestModel();
    private MockLoadTestServingModelManager(Config config) {
      super(config);
    }
    @Override
    public ALSServingModel getModel() {
      return model;
    }
  }

}
