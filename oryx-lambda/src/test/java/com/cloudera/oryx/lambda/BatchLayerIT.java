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

package com.cloudera.oryx.lambda;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Tests {@link BatchLayer}.
 */
public final class BatchLayerIT extends AbstractBatchIT {

  private static final Logger log = LoggerFactory.getLogger(BatchLayerIT.class);

  private static final int DATA_TO_WRITE = 600;
  private static final int WRITE_INTERVAL_MSEC = 20;
  private static final int GEN_INTERVAL_SEC = 3;
  private static final int BLOCK_INTERVAL_SEC = 1;

  @Test
  public void testBatchLayer() throws Exception {
    doTestBatchLayer(2, 1);
  }

  @Test
  public void testBatchLayerMultipleReceivers() throws Exception {
    doTestBatchLayer(10, 10);
  }

  private void doTestBatchLayer(int partitions, int receivers) throws IOException, InterruptedException {
    Path tempDir = getTempDir();
    Path dataDir = tempDir.resolve("data");
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", MockBatchUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", tempDir.resolve("model"));
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    overlayConfig.put("oryx.batch.streaming.block-interval-sec", BLOCK_INTERVAL_SEC);
    overlayConfig.put("oryx.batch.storage.partitions", partitions);
    overlayConfig.put("oryx.batch.streaming.receiver-parallelism", receivers);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    List<IntervalData<String,String>> intervalData = MockBatchUpdate.getIntervalDataHolder();
    intervalData.clear();

    startMessaging();

    startServerProduceConsumeTopics(config, DATA_TO_WRITE, WRITE_INTERVAL_MSEC);

    int numIntervals = intervalData.size();
    log.info("{} intervals: {}", numIntervals, intervalData);

    checkOutputData(dataDir, DATA_TO_WRITE);
    checkIntervals(numIntervals, DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    IntervalData<String,String> last = intervalData.get(0);
    log.info("Interval 0: {}", last);
    for (int i = 1; i < numIntervals; i++) {
      IntervalData<String,String> current = intervalData.get(i);
      log.info("Interval {}: {}", i, current);
      assertTrue(current.getTimestamp() > last.getTimestamp());
      assertTrue(current.getPastData().size() >= last.getPastData().size());
      assertEquals(last.getPastData().size() + last.getCurrentData().size(),
                   current.getPastData().size());
      last = current;
    }
  }

}
