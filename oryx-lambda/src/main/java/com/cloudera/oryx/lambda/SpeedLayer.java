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

package com.cloudera.oryx.lambda;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for Oryx Speed Layer.
 *
 * @param <K> type of key read from input queue
 * @param <M> type of message read from input queue
 * @param <U> type of update message written
 */
public final class SpeedLayer<K,M,U> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(SpeedLayer.class);

  private final String streamingMaster;
  private final String queueLockMaster;
  private final String messageTopic;
  private final int generationIntervalSec;
  private final int blockIntervalSec;
  private JavaStreamingContext streamingContext;

  public SpeedLayer(Config config) {
    Preconditions.checkNotNull(config);
    this.streamingMaster = config.getString("speed.streaming.master");
    this.queueLockMaster = config.getString("input-queue.lock.master");
    this.messageTopic = config.getString("input-queue.message.topic");
    this.generationIntervalSec = config.getInt("speed.generation-interval-sec");
    this.blockIntervalSec = config.getInt("speed.block-interval-sec");
  }

  public synchronized void start() {
    log.info("Starting SparkContext for master {}, interval {} seconds",
             streamingMaster, generationIntervalSec);

    long blockIntervalMS = TimeUnit.MILLISECONDS.convert(blockIntervalSec, TimeUnit.SECONDS);

    SparkConf sparkConf = new SparkConf();
    sparkConf.setIfMissing("spark.streaming.blockInterval", Long.toString(blockIntervalMS));
    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("OryxSpeedLayer");
    long batchDurationMS = TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);
    streamingContext = new JavaStreamingContext(sparkConf, new Duration(batchDurationMS));

    log.info("Creating message queue stream");

    JavaPairDStream<K,M> dStream = buildDStream();


    log.info("Starting streaming");

    streamingContext.start();
  }

  public void await() {
    Preconditions.checkState(streamingContext != null);
    log.info("Waiting for streaming...");
    streamingContext.awaitTermination();
  }

  @Override
  public synchronized void close() {
    if (streamingContext != null) {
      log.info("Shutting down");
      streamingContext.stop(true, true);
      streamingContext = null;
    }
  }

  private JavaPairDStream<K,M> buildDStream() {
    // TODO for now we can only support default of Strings
    @SuppressWarnings("unchecked")
    JavaPairDStream<K,M> dStream = (JavaPairDStream<K,M>)
        KafkaUtils.createStream(streamingContext,
                                queueLockMaster,
                                // group should be unique
                                "OryxGroup-SpeedLayer-" + System.currentTimeMillis(),
                                Collections.singletonMap(messageTopic, 1));
    return dStream;
  }


}
