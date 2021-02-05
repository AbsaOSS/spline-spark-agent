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

import org.apache.commons.configuration.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.spark.internal.Logging
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.ProducerApiVersion
import za.co.absa.spline.harvester.dispatcher.kafkadispatcher.{KafkaLineageDispatcherConfig, _}
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.util.control.NonFatal

/**
 * KafkaLineageDispatcher is responsible for sending the lineage data to spline gateway through kafka
 */
class KafkaLineageDispatcher(config: KafkaLineageDispatcherConfig)
  extends LineageDispatcher
    with Logging {

  def this(configuration: Configuration) = this(KafkaLineageDispatcherConfig(configuration))

  private val producer = new KafkaProducer[String, String](config.kafkaProducerProperties)
  private val planTopic = config.planTopic
  private val eventTopic = config.eventTopic

  logInfo(s"Kafka execution plan topic: $planTopic")
  logInfo(s"Kafka execution event topic: $eventTopic")

  sys.addShutdownHook(producer.close())

  private val kafkaHeaders = Seq[Header](
    new KafkaHeader(SplineKafkaHeaders.ApiVersion, ProducerApiVersion.Default.asString)
  )

  override def send(plan: ExecutionPlan): Unit = {
    sendRecord(new ProducerRecord(
      planTopic,
      null,
      plan.id.get.toString,
      plan.toJson,
      kafkaHeaders.asJava
    ))
  }

  override def send(event: ExecutionEvent): Unit = {
    sendRecord(new ProducerRecord(
      eventTopic,
      null,
      event.planId.toString,
      event.toJson,
      kafkaHeaders.asJava
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
