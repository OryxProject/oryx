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

package com.cloudera.oryx.example.speed

import scala.collection.mutable
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import com.cloudera.oryx.api.speed.AbstractScalaSpeedModelManager
import com.cloudera.oryx.example.batch.ExampleScalaBatchLayerUpdate
import com.typesafe.config.Config

/**
 * Also counts and emits counts of number of distinct words that occur with words.
 * Listens for updates from the Batch Layer, which give the current correct count at its
 * last run. Updates these counts approximately in response to the same data stream
 * that the Batch Layer sees, but assumes all words seen are new and distinct, which is only
 * approximately true. Emits updates of the form "word,count".
 */
class ExampleScalaSpeedModelManager(val config: Config)
  extends AbstractScalaSpeedModelManager[String,String,String](config) {

  private val distinctOtherWords = mutable.Map[String,Integer]()

  override def consumeKeyMessage(key: String, message: String, hadoopConf: Configuration) = {
    key match {
      case "MODEL" =>
        val model =
          new ObjectMapper().readValue(message, classOf[java.util.Map[String,String]]).asScala
        distinctOtherWords.synchronized {
          distinctOtherWords.clear()
          model.foreach { case (word, count) =>
            distinctOtherWords.put(word, count.toInt)
          }
        }
      case _ => // ignore
    }
  }

  override def buildUpdates(newData: RDD[(String,String)]) = {
    ExampleScalaBatchLayerUpdate.countDistinctOtherWords(newData).map { case (word, count) =>
      distinctOtherWords.synchronized {
        val newCount = distinctOtherWords(word) + 1
        distinctOtherWords(word) = newCount
        word + "," + newCount
      }
    }.toSeq
  }

}
