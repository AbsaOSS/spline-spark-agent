/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.harvester.plugin.impl

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.kafka010.{AssignStrategy, ConsumerStrategy, SubscribePatternStrategy, SubscribeStrategy}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.{DataSourceFormatResolver, SourceIdentifier}
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.impl.KafkaPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationPlugin, DataSourceTypePlugin, Plugin}

import scala.collection.JavaConverters._

class KafkaPlugin
  extends Plugin
    with BaseRelationPlugin
    with DataSourceTypePlugin {

  override def baseRelProcessor: PartialFunction[(BaseRelation, LogicalRelation), (SourceIdentifier, Params)] = {
    case (`_: KafkaRelation`(kr), _) =>
      val options = extractFieldValue[Map[String, String]](kr, "sourceOptions")
      val topics: Seq[String] = extractFieldValue[ConsumerStrategy](kr, "strategy") match {
        case AssignStrategy(partitions) => partitions.map(_.topic)
        case SubscribeStrategy(topics) => topics
        case SubscribePatternStrategy(pattern) => kafkaTopics(options("kafka.bootstrap.servers")).filter(_.matches(pattern))
      }
      val sourceId = SourceIdentifier(Some("kafka"), topics.map(asURI): _*)
      (sourceId, options ++ Map(
        "startingOffsets" -> extractFieldValue[AnyRef](kr, "startingOffsets"),
        "endingOffsets" -> extractFieldValue[AnyRef](kr, "endingOffsets")
      ))
  }

  override def dataSourceTypeProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), (SourceIdentifier, SaveMode, LogicalPlan, Params)] = {
    case (st, cmd) if cmd.options.contains("kafka.bootstrap.servers") =>
      val formatName = DataSourceFormatResolver.resolve(st)
      val uri = asURI(cmd.options("topic"))
      (SourceIdentifier(Some(formatName), uri), cmd.mode, cmd.query, cmd.options)
  }
}

object KafkaPlugin {

  object `_: KafkaRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.kafka010.KafkaRelation")

  private def kafkaTopics(bootstrapServers: String): Seq[String] = {
    val kc = new KafkaConsumer(new Properties {
      put("bootstrap.servers", bootstrapServers)
      put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    })
    try kc.listTopics.keySet.asScala.toSeq
    finally kc.close()
  }

  private def asURI(topic: String) = s"kafka:$topic"
}
