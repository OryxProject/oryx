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

import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.ServletContextListener;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.JVMUtils;
import com.cloudera.oryx.common.lang.LoggingVoidCallable;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.app.serving.als.model.LoadTestALSModelFactory;

public final class LoadIT extends AbstractALSServingTest {

  private static final Logger log = LoggerFactory.getLogger(LoadIT.class);
  private static final int REQS_PER_WORKER = 100;
  private static final int WORKERS = 1;

  @Override
  protected void configureProperties() {
    super.configureProperties();
    disable(TestProperties.LOG_TRAFFIC);
    disable(TestProperties.DUMP_ENTITY);
  }

  @Ignore("Difficult to assert about time in cross-platform way; run manually")
  @Test
  public void testContainerOverhead() {
    long startNS = System.nanoTime();
    int requests = 10000;
    for (int i = 0; i < requests; i++) {
      try {
        target("/").request().get(LIST_ID_VALUE_TYPE);
      } catch (NotFoundException nfe) {
        // continue
      }
    }
    long usecPerRequest = Math.round((System.nanoTime() - startNS) / (requests * 1000.0));
    log.info("{} microseconds / request", usecPerRequest);
    Assert.assertTrue(usecPerRequest < 3000);
  }

  @Ignore("Difficult to assert about time in cross-platform way; run manually")
  @Test
  public void testRecommendLoad() throws Exception {
    AtomicLong count = new AtomicLong();
    Mean meanReqTimeMS = new Mean();
    long start = System.currentTimeMillis();

    /*
    List<Callable<Void>> tasks = new ArrayList<>(workers);
    for (int i = 0; i < workers; i++) {
      tasks.add(new LoadCallable(Integer.toString(i), meanReqTimeMS, count, start));
    }

    ExecutorService executor = Executors.newFixedThreadPool(WORKERS);
    try {
      executor.invokeAll(tasks);
    } finally {
      executor.shutdown();
    }
     */
    // Since latency is more important, and local machine will also be busy handling requests,
    // use few concurrent workers, like 1:
    new LoadCallable("0", meanReqTimeMS, count, start).call();

    int totalRequests = WORKERS * REQS_PER_WORKER;
    log(totalRequests, meanReqTimeMS, start);

    int cores = Runtime.getRuntime().availableProcessors();
    int allowedMS = 150 + 640 / cores; // crude, conservative empirical limit
    Assert.assertTrue(
        "Expected < " + allowedMS + "ms / req with " + cores + " cores",
        meanReqTimeMS.getResult() < allowedMS);
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
      return new MockLoadTestServingModelManager();
    }
  }

  private static final class MockLoadTestServingModelManager
      extends AbstractALSServingTest.MockServingModelManager {
    private final ALSServingModel model = LoadTestALSModelFactory.buildTestModel();
    @Override
    public ALSServingModel getModel() {
      return model;
    }
  }

  private final class LoadCallable extends LoggingVoidCallable {

    private final RandomGenerator random;
    private final Mean meanReqTimeMS;
    private final AtomicLong count;
    private final long start;

    private LoadCallable(String id, Mean meanReqTimeMS, AtomicLong count, long start) {
      this.meanReqTimeMS = meanReqTimeMS;
      this.count = count;
      this.start = start;
      random = RandomManager.getRandom();
      // We do *not* want a deterministic seed here!
      random.setSeed(id.hashCode() ^ System.nanoTime());
    }

    @Override
    public void doCall() {
      for (int j = 0; j < REQS_PER_WORKER; j++) {
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
    }
  }

}
