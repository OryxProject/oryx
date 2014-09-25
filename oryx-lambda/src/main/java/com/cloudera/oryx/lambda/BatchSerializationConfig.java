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

import java.io.Serializable;

import com.typesafe.config.Config;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import org.apache.hadoop.io.Writable;

import com.cloudera.oryx.common.lang.ClassUtils;

/**
 * Encapsulates configuration of classes used in reading data from the input queue,
 * and also persisting the data to storage in Hadoop.
 */
public final class BatchSerializationConfig implements Serializable {

  private final Class<? extends Decoder<?>> keyDecoderClass;
  private final Class<? extends Decoder<?>> messageDecoderClass;
  private final Class<? extends Encoder<?>> keyEncoderClass;
  private final Class<? extends Encoder<?>> messageEncoderClass;
  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;

  @SuppressWarnings("unchecked")
  public BatchSerializationConfig(Config config) {
    this.keyDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("input-queue.message.key-decoder-class"), Decoder.class);
    this.messageDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("input-queue.message.message-decoder-class"), Decoder.class);
    this.keyEncoderClass = (Class<? extends Encoder<?>>) ClassUtils.loadClass(
        config.getString("input-queue.message.key-encoder-class"), Encoder.class);
    this.messageEncoderClass = (Class<? extends Encoder<?>>) ClassUtils.loadClass(
        config.getString("input-queue.message.message-encoder-class"), Encoder.class);
    this.keyWritableClass = ClassUtils.loadClass(
        config.getString("batch.storage.key-writable-class"), Writable.class);
    this.messageWritableClass = ClassUtils.loadClass(
        config.getString("batch.storage.message-writable-class"), Writable.class);
  }

  public Class<? extends Decoder<?>> getKeyDecoderClass() {
    return keyDecoderClass;
  }

  public Class<? extends Decoder<?>> getMessageDecoderClass() {
    return messageDecoderClass;
  }

  public Class<? extends Encoder<?>> getKeyEncoderClass() {
    return keyEncoderClass;
  }

  public Class<? extends Encoder<?>> getMessageEncoderClass() {
    return messageEncoderClass;
  }

  public Class<? extends Writable> getKeyWritableClass() {
    return keyWritableClass;
  }

  public Class<? extends Writable> getMessageWritableClass() {
    return messageWritableClass;
  }

}
