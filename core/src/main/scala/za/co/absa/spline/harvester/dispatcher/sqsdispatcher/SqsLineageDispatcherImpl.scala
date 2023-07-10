package za.co.absa.spline.harvester.dispatcher.sqsdispatcher

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import za.co.absa.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.dispatcher.modelmapper.ModelMapper
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

class SqsLineageDispatcherImpl(sqsClient: SqsClient,
                               sqsUrl: String,
                               apiVersion: Version) extends LineageDispatcher with Logging {
  import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
  def this(dispatcherConfig: SqsLineageDispatcherConfig) = this(
    SqsLineageDispatcherImpl.createSqsClient(dispatcherConfig),
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


object SqsLineageDispatcherImpl extends Logging {

  private def createSqsClient(config: SqsLineageDispatcherConfig): SqsClient = {
    SqsClient
      .builder()
      .build()
  }
}
