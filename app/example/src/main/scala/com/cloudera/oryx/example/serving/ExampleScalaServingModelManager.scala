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

package com.cloudera.oryx.example.serving

import com.typesafe.config.Config

import scala.collection.mutable
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration

import com.cloudera.oryx.api.serving.{ServingModel, AbstractScalaServingModelManager}

/**
 * Reads models and updates produced by the Batch Layer and Speed Layer. Models are maps, encoded as JSON
 * strings, mapping words to count of distinct other words that appear with that word in an input line.
 * Updates are "word,count" pairs representing new counts for a word. This class manages and exposes the
 * mapping to the Serving Layer applications.
 */
class ExampleScalaServingModelManager(val config: Config)
    extends AbstractScalaServingModelManager[String](config) {

  private val distinctOtherWords = mutable.Map[String,Integer]()

  override def consumeKeyMessage(key: String, message: String, hadoopConf: Configuration): Unit = {
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
      case "UP" =>
        val Array(word, count) = message.split(",")
        distinctOtherWords.synchronized(
          distinctOtherWords.put(word, count.toInt)
        )
    }
  }

  override def getModel: ServingModel = new ExampleServingModel(distinctOtherWords.asJava)

}
