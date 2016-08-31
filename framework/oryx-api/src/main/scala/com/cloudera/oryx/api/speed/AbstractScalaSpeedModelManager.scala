/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.api.speed

import com.cloudera.oryx.api.KeyMessage
import com.cloudera.oryx.api.serving.ScalaServingModelManager
import com.typesafe.config.Config
import java.util.Objects
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

/**
 * Convenience implementation of [[ScalaSpeedModelManager]] that provides several default implementations.
 *
 * @tparam K type of key read from input topic
 * @tparam M type of message read from input topic
 * @tparam U type of update message read/written
 * @since 2.3.0
 */
abstract class AbstractScalaSpeedModelManager[K,M,U](private val config: Config) extends ScalaSpeedModelManager[K,M,U] {

  import AbstractScalaSpeedModelManager._

  override def consume(updateIterator: Iterator[KeyMessage[String,U]], hadoopConf: Configuration): Unit = {
    updateIterator.foreach { km =>
      val key = km.getKey
      val message = km.getMessage
      try {
        Objects.requireNonNull(key)
        consumeKeyMessage(key, message, hadoopConf)
      } catch {
        case e: Exception =>
          log.warn("Exception while processing message", e)
          log.warn("Key/message were {} : {}", key, message)
      }
    }
  }

  /**
   * Convenience method that is called by the default implementation of
   * [[ScalaSpeedModelManager.consume()]], to process one key-message pair.
   * It does nothing, except log the message. This should generally be overridden
   * if and only if [[ScalaServingModelManager.consume()]] is not.
   *
   * @param key key to process (non-null)
   * @param message message to process
   * @param hadoopConf Hadoop configuration for process
   * @since 2.3.0
   */
  def consumeKeyMessage(key: String, message: U, hadoopConf: Configuration): Unit = {
    log.info("{} : {}", key, message)
  }

}

object AbstractScalaSpeedModelManager {
  val log = LoggerFactory.getLogger(classOf[AbstractScalaSpeedModelManager[_,_,_]])
}
