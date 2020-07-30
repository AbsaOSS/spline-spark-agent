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

package za.co.absa.spline.harvester.plugin.embedded

import java.util.Properties

import javax.annotation.Priority
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.kafka010.{AssignStrategy, ConsumerStrategy, SubscribePatternStrategy, SubscribeStrategy}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.KafkaPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin, RelationProviderProcessing}

import scala.collection.JavaConverters._

@Priority(Precedence.Normal)
class KafkaPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing {

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
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

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (st, cmd) if cmd.options.contains("kafka.bootstrap.servers") =>
      val uri = asURI(cmd.options("topic"))
      (SourceIdentifier(Option(st), uri), cmd.mode, cmd.query, cmd.options)
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
