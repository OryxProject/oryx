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

package com.cloudera.oryx.lambda.batch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.kafka.util.ConsumeData;
import com.cloudera.oryx.kafka.util.ConsumeTopicRunnable;
import com.cloudera.oryx.kafka.util.DefaultCSVDatumGenerator;
import com.cloudera.oryx.kafka.util.ProduceData;
import com.cloudera.oryx.kafka.util.DatumGenerator;
import com.cloudera.oryx.lambda.AbstractLambdaIT;

public abstract class AbstractBatchIT extends AbstractLambdaIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractBatchIT.class);

  protected List<KeyMessage<String,String>> startServerProduceConsumeTopics(
      Config config,
      int howMany,
      int intervalMsec) throws IOException, InterruptedException {
    return startServerProduceConsumeTopics(config,
                                           new DefaultCSVDatumGenerator(),
                                           howMany,
                                           intervalMsec);
  }

  protected List<KeyMessage<String,String>> startServerProduceConsumeTopics(
      Config config,
      DatumGenerator<String,String> datumGenerator,
      int howMany,
      int intervalMsec) throws IOException, InterruptedException {

    int zkPort = getZKPort();

    ProduceData produce = new ProduceData(datumGenerator,
                                          getKafkaBrokerPort(),
                                          INPUT_TOPIC,
                                          howMany,
                                          intervalMsec);

    List<KeyMessage<String,String>> keyMessages;
    try (CloseableIterator<KeyMessage<String,String>> data =
             new ConsumeData(UPDATE_TOPIC, zkPort).iterator();
         BatchLayer<?,?,?> batchLayer = new BatchLayer<>(config)) {

      log.info("Starting batch layer");
      batchLayer.start();

      // Sleep to let batch layer start
      sleepSeconds(3);

      log.info("Starting consumer thread");
      ConsumeTopicRunnable consumeInput = new ConsumeTopicRunnable(data);
      new Thread(LoggingCallable.log(consumeInput).asRunnable(), "ConsumeInputThread").start();

      consumeInput.awaitRun();

      log.info("Producing data");
      produce.start();

      // Sleep generation before shutting down server to let it finish
      int genIntervalSec = config.getInt("oryx.batch.streaming.generation-interval-sec");
      sleepSeconds(genIntervalSec);

      keyMessages = consumeInput.getKeyMessages();
    }

    return keyMessages;
  }


  protected static void checkOutputData(Path dataDir, int expectedCount) throws IOException {
    List<Path> dataFiles = IOUtils.listFiles(dataDir, "*/part-*");
    Configuration hadoopConf = new Configuration();
    Writable key = new Text();
    Writable message = new Text();
    int count = 0;
    for (Path dataFile : dataFiles) {
      SequenceFile.Reader.Option fileOptions =
          SequenceFile.Reader.file(new org.apache.hadoop.fs.Path(dataFile.toUri().toString()));
      try (SequenceFile.Reader reader = new SequenceFile.Reader(hadoopConf, fileOptions)) {
        while (reader.next(key, message)) {
          count++;
        }
      }
    }
    log.info("Read {} data", count);
    if (expectedCount != count) {
      log.warn("Bad count of data read, dumping data...");
      for (Path dataFile : dataFiles) {
        log.info("Data file: {}", dataFile);
        SequenceFile.Reader.Option fileOptions =
            SequenceFile.Reader.file(new org.apache.hadoop.fs.Path(dataFile.toUri().toString()));
        try (SequenceFile.Reader reader = new SequenceFile.Reader(hadoopConf, fileOptions)) {
          while (reader.next(key, message)) {
            log.info("  {} = {}", key, message);
          }
        }
      }
    }
    assertEquals("Wrong count of data read", expectedCount, count);
  }

  protected static void checkIntervals(int numIntervals,
                                       int dataToWrite,
                                       int writeIntervalMsec,
                                       int genIntervalSec) {
    int minExpectedIntervals = dataToWrite * writeIntervalMsec / (1000 * genIntervalSec);
    // Can be up to +4 depending on distribution and timing, and because the framework
    // waits a while before starting to send data and after last data is sent before
    // stopping the streaming system
    assertTrue(
        "Expected " + minExpectedIntervals +
            " intervals (or up to +4 more) but got " + numIntervals,
        numIntervals >= minExpectedIntervals && numIntervals <= minExpectedIntervals + 4);
  }

}
