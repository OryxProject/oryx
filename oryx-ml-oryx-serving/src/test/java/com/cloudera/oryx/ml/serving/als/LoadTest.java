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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.ServletContextListener;

import com.google.common.base.Stopwatch;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.JVMUtils;
import com.cloudera.oryx.common.lang.LoggingVoidCallable;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;
import com.cloudera.oryx.ml.serving.als.model.LoadTestALSModelFactory;

public final class LoadTest extends AbstractALSServingTest {

  private static final Logger log = LoggerFactory.getLogger(LoadTest.class);
  private static final int REQS_PER_WORKER = 100;

  @Override
  protected Class<?> getResourceClass() {
    return Recommend.class;
  }

  @Test
  public void testRecommendLoad() throws Exception {
    int workers = Runtime.getRuntime().availableProcessors();
    final AtomicLong count = new AtomicLong();
    final Mean meanReqTimeMS = new Mean();
    final Stopwatch stopwatch = new Stopwatch();

    List<Callable<Void>> tasks = new ArrayList<>(workers);
    for (int i = 0; i < workers; i++) {
      tasks.add(new LoggingVoidCallable() {
        private final RandomGenerator random = RandomManager.getRandom();
        @Override
        public void doCall() {
          for (int j = 0; j < REQS_PER_WORKER; j++) {
            String userID = "U" + random.nextInt(LoadTestALSModelFactory.USERS);
            long start = System.currentTimeMillis();
            target("/recommend/" + userID).request().get(LIST_ID_VALUE_TYPE);
            long end = System.currentTimeMillis();
            synchronized (meanReqTimeMS) {
              meanReqTimeMS.increment(end - start);
            }
            long currentCount = count.incrementAndGet();
            if (currentCount % 100 == 0) {
              log(currentCount, meanReqTimeMS, stopwatch);
            }
          }
        }
      });
    }

    ExecutorService executor = Executors.newFixedThreadPool(workers);

    stopwatch.start();
    try {
      executor.invokeAll(tasks);
    } finally {
      executor.shutdown();
    }
    stopwatch.stop();

    int totalRequests = workers * REQS_PER_WORKER;
    log(totalRequests, meanReqTimeMS, stopwatch);

    // Need to get this down!
    Assert.assertTrue(meanReqTimeMS.getResult() < 800.0);
  }

  private static void log(long currentCount, Mean meanReqTimeMS, Stopwatch stopwatch) {
    long used = JVMUtils.getUsedMemory() / 1_000_000;
    long requestsPerSecond =
        Math.round((double) currentCount / stopwatch.elapsed(TimeUnit.SECONDS));
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

}
