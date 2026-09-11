/*
 * Copyright 2019 ABSA Group Limited
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
import org.apache.spark.internal.Logging
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import za.co.absa.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.modelmapper.ModelMapper
import za.co.absa.spline.harvester.dispatcher.sqsdispatcher.SqsLineageDispatcherConfig
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

class SqsLineageDispatcher(sqsClient: SqsClient,
                           sqsUrl: String,
                           apiVersion: Version) extends LineageDispatcher with Logging {
  import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
  def this(dispatcherConfig: SqsLineageDispatcherConfig) = this(
    SqsLineageDispatcher.createSqsClient(dispatcherConfig),
    dispatcherConfig.queueUrl,
    dispatcherConfig.apiVersion
  )

  def this(configuration: Configuration) = this(new SqsLineageDispatcherConfig(configuration))

  override def name = "Sqs"

  logInfo(s"Using Producer API version: ${apiVersion.asString}")
  logInfo(s"Sqs url: $sqsUrl")

  private val modelMapper = ModelMapper.forApiVersion(apiVersion)

  private var cachedPlan: ExecutionPlan = _

  override def send(plan: ExecutionPlan): Unit = {
    cachedPlan = plan
  }

  override def send(event: ExecutionEvent): Unit = {
    assert(cachedPlan != null)
    val plan = cachedPlan
    for {
      execPlanDTO <- modelMapper.toDTO(plan)
      eventDTO <- modelMapper.toDTO(event)
    }  {
      val jsonPlan = execPlanDTO.toJson
      val jsonEvent = eventDTO.toJson
      val json =
        s"""
           | {
           |   "plan": $jsonPlan,
           |   "event": $jsonEvent
           | }
           |""".stripMargin
      sendToSqs(json)
    }
  }

  private def sendToSqs(json: String,
                        objectType: String = "Spline"): Unit = {
    val body =
      s"""
         | { "requestType": "SparkJobRunInfo",
         |   "objectType": "$objectType",
         |   "body": $json
         | }
         |""".stripMargin
    val sendMsgRequest = SendMessageRequest.builder()
      .queueUrl(sqsUrl)
      .messageBody(body)
      .build()
    sqsClient.sendMessage(sendMsgRequest)
  }
}


object SqsLineageDispatcher extends Logging {

  private def createSqsClient(config: SqsLineageDispatcherConfig): SqsClient = {
    SqsClient
      .builder()
      .build()
  }
}
