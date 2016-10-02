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

package com.cloudera.oryx.example.batch

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.cloudera.oryx.api.TopicProducer
import com.cloudera.oryx.api.batch.ScalaBatchLayerUpdate

/**
 * Input keys are ignored. Values are treated as lines of space-separated text. The job
 * counts, for each word, the number of distinct other words that co-occur in some line
 * of text in the input. These are written as a "MODEL" update, where the word-count mapping
 * is written as a JSON string.
 */
class ExampleScalaBatchLayerUpdate extends ScalaBatchLayerUpdate[String,String,String] {

  override def configureUpdate(sparkContext: SparkContext,
                               timestamp: Long,
                               newData: RDD[(String,String)],
                               pastData: RDD[(String,String)],
                               modelDirString: String,
                               modelUpdateTopic: TopicProducer[String,String]) = {
    val modelString = new ObjectMapper().writeValueAsString(
        ExampleScalaBatchLayerUpdate.countDistinctOtherWords(newData.union(pastData)).asJava)
    modelUpdateTopic.send("MODEL", modelString)
  }

}

object ExampleScalaBatchLayerUpdate {

  def countDistinctOtherWords(data: RDD[(String,String)]) = {
    data.values.flatMap { line =>
      val tokens = line.split(" ").distinct
      for (a <- tokens; b <- tokens if a != b) yield (a, b)
    }.distinct().groupByKey().mapValues(_.size).collectAsMap()
  }

}
