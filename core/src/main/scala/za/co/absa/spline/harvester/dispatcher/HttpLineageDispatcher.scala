/*
 * Copyright 2019 ABSA Group Limited
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
import scalaj.http.{Http, HttpStatusException}
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpConstants.Encoding
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.ProducerApiVersion.SupportedApiRange
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.modelmapper.ModelMapper
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.rest.{RestClient, RestEndpoint}
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.{ProducerApiCompatibilityManager, ProducerApiVersion}
import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

object HttpLineageDispatcher extends Logging {
  private val ProducerUrlProperty = "spline.producer.url"
  private val ConnectionTimeoutMsKey = "spline.timeout.connection"
  private val ReadTimeoutMsKey = "spline.timeout.read"

  private val DefaultConnectionTimeout: Duration = 1.second
  private val DefaultReadTimeout: Duration = 20.second

  private object RESTResource {
    val ExecutionPlans = "execution-plans"
    val ExecutionEvents = "execution-events"
    val Status = "status"
  }

  private object SplineHttpHeaders {
    private val Prefix = "ABSA-Spline"

    val ApiVersion = s"$Prefix-API-Version"
    val ApiLTSVersion = s"$Prefix-API-LTS-Version"
    val AcceptRequestEncoding = s"$Prefix-Accept-Request-Encoding"
  }

  private def createHttpErrorMessage(msg: String, code: Int, body: String): String =
    s"$msg. HTTP Response: $code $body"

  private def defaultRestClient(c: Configuration): RestClient = {
    val producerUrl = c.getRequiredString(ProducerUrlProperty)
    val connTimeout = c.getLong(ConnectionTimeoutMsKey, DefaultConnectionTimeout.toMillis).millis
    val readTimeout = c.getLong(ReadTimeoutMsKey, DefaultReadTimeout.toMillis).millis

    logInfo(s"Producer URL: $producerUrl")

    RestClient(
      Http,
      producerUrl,
      connTimeout,
      readTimeout
    )
  }
}

/**
 * HttpLineageDispatcher is responsible for sending the lineage data to spline gateway through producer API
 */
class HttpLineageDispatcher(restClient: RestClient)
  extends LineageDispatcher
    with Logging {

  import za.co.absa.spline.harvester.dispatcher.HttpLineageDispatcher._

  def this(configuration: Configuration) = this(HttpLineageDispatcher.defaultRestClient(configuration))

  private val statusEndpoint = restClient.endpoint(RESTResource.Status)
  private val executionPlansEndpoint = restClient.endpoint(RESTResource.ExecutionPlans)
  private val executionEventsEndpoint = restClient.endpoint(RESTResource.ExecutionEvents)

  private val serverHeaders: Map[String, IndexedSeq[String]] = {
    val unableToConnectMsg = "Spark Agent was not able to establish connection to Spline Gateway"
    val serverHasIssuesMsg = "Connection to Spline Gateway: OK, but the Gateway is not initialized properly! Check Gateway logs"

    Try(statusEndpoint.head())
      .map {
        case resp if resp.is2xx =>
          resp.headers
        case resp if resp.is5xx =>
          throw new SplineInitializationException(createHttpErrorMessage(serverHasIssuesMsg, resp.code, resp.body))
        case resp =>
          throw new SplineInitializationException(createHttpErrorMessage(unableToConnectMsg, resp.code, resp.body))
      }
      .recover {
        case e: SplineInitializationException => throw e
        case NonFatal(e) => throw new SplineInitializationException(unableToConnectMsg, e)
      }
      .get
      .withDefaultValue(Array.empty[String])
  }

  private val modelMapper = ModelMapper.forApiVersion({
    val serverApiVersions =
      serverHeaders(SplineHttpHeaders.ApiVersion)
        .map(Version.asSimple)
        .asOption
        .getOrElse(Seq(ProducerApiVersion.Default))

    val serverApiLTSVersions =
      serverHeaders(SplineHttpHeaders.ApiLTSVersion)
        .map(Version.asSimple)
        .asOption
        .getOrElse(serverApiVersions)

    val apiCompatManager = ProducerApiCompatibilityManager(serverApiVersions, serverApiLTSVersions)

    apiCompatManager.newerServerApiVersion.foreach(ver => logWarning(s"Newer Producer API version ${ver.asString} is available on the server"))
    apiCompatManager.deprecatedApiVersion.foreach(ver => logWarning(s"UPGRADE SPLINE AGENT! Producer API version ${ver.asString} is deprecated"))

    val apiVersion = apiCompatManager.highestCompatibleApiVersion.getOrElse(throw new SplineInitializationException(
      s"Spline Agent and Server versions don't match. " +
        s"Agent supports API versions ${SupportedApiRange.Min.asString} to ${SupportedApiRange.Max.asString}, " +
        s"but the server only provides: ${serverApiVersions.map(_.asString).mkString(", ")}"))

    logInfo(s"Using Producer API version: ${apiVersion.asString}")

    apiVersion
  })

  private val requestCompressionSupported: Boolean =
    serverHeaders(SplineHttpHeaders.AcceptRequestEncoding)
      .exists(_.toLowerCase == Encoding.GZIP)

  override def send(executionPlan: ExecutionPlan): String = {
    val execPlanDTO = modelMapper.toDTO(executionPlan)
    sendJson(execPlanDTO.toJson, executionPlansEndpoint)
  }

  override def send(event: ExecutionEvent): Unit = {
    val eventDTO = modelMapper.toDTO(event)
    sendJson(Seq(eventDTO).toJson, executionEventsEndpoint)
  }

  private def sendJson(json: String, endpoint: RestEndpoint) = {
    val url = endpoint.request.url
    logTrace(s"sendJson $url : $json")

    try {
      endpoint
        .post(json, requestCompressionSupported)
        .throwError
        .body

    } catch {
      case HttpStatusException(code, _, body) =>
        throw new RuntimeException(createHttpErrorMessage(s"Cannot send lineage data to $url", code, body))
      case NonFatal(e) =>
        throw new RuntimeException(s"Cannot send lineage data to $url", e)
    }
  }
}
