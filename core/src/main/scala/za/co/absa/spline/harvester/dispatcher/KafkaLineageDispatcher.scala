/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.{Configuration, ConfigurationConverter}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.commons.config.ConfigurationImplicits.ConfigurationRequiredWrapper
import za.co.absa.spline.harvester.dispatcher.KafkaLineageDispatcher._
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.ProducerApiVersion
import za.co.absa.spline.harvester.dispatcher.kafkadispatcher._
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

import java.util.Properties
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.util.control.NonFatal

/**
 * KafkaLineageDispatcher is responsible for sending the lineage data to spline gateway through kafka
 */
class KafkaLineageDispatcher(topic: String, producerProperties: Properties, sparkSession: SparkSession)
  extends LineageDispatcher
    with Logging {

  def this(configuration: Configuration) = this(
    configuration.getRequiredString(TopicKey),
    ConfigurationConverter.getProperties(configuration.subset(ProducerKey)),
    configuration.getProperty("sparkSession").asInstanceOf[SparkSession]
  )

  logInfo(s"Kafka topic: $topic")

  private val producer = new KafkaProducer[String, String](producerProperties)
  sparkSession.sparkContext.addSparkListener(new AppEndListener(() => producer.close()))

  private val planKafkaHeaders = Seq[Header](
    new KafkaHeader(SplineKafkaHeaders.ApiVersion, ProducerApiVersion.Default.asString),
    new KafkaHeader(SplineKafkaHeaders.TypeId, "ExecutionPlan")
  )

  private val eventKafkaHeaders = Seq[Header](
    new KafkaHeader(SplineKafkaHeaders.ApiVersion, ProducerApiVersion.Default.asString),
    new KafkaHeader(SplineKafkaHeaders.TypeId, "ExecutionEvent")
  )

  override def send(plan: ExecutionPlan): Unit = {
    sendRecord(new ProducerRecord(
      topic,
      null,
      plan.id.get.toString,
      plan.toJson,
      planKafkaHeaders.asJava
    ))
  }

  override def send(event: ExecutionEvent): Unit = {
    sendRecord(new ProducerRecord(
      topic,
      null,
      event.planId.toString,
      event.toJson,
      eventKafkaHeaders.asJava
    ))
  }

  private def sendRecord(record: ProducerRecord[String, String]) = {
    logTrace(s"sending message to kafka topic: ${record.topic}\n"
      + "key: ${record.key}\nvalue: ${record.value.asPrettyJson}")

    try {
      producer.send(record).get()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Cannot send lineage data to kafka topic ${record.topic()}", e)
    }
  }
}

object KafkaLineageDispatcher {
  private val TopicKey = "topic"
  private val ProducerKey = "producer"
}
