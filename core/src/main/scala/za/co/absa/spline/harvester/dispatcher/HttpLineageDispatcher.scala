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


import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

import javax.ws.rs.HttpMethod
import javax.ws.rs.core.MediaType
import org.apache.commons.configuration.Configuration
import org.apache.http.HttpHeaders
import org.apache.spark.internal.Logging
import scalaj.http.{BaseHttp, Http}
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.commons.lang.ARM.using
import za.co.absa.spline.harvester.dispatcher.HttpLineageDispatcher.{RESTResource, _}
import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

object HttpLineageDispatcher {
  val ProducerUrlProperty = "spline.producer.url"
  val ConnectionTimeoutMsKey = "spline.timeout.connection"
  val ReadTimeoutMsKey = "spline.timeout.read"

  val DefaultConnectionTimeout: Duration = 1.second
  val DefaultReadTimeout: Duration = 20.second

  object RESTResource {
    val ExecutionPlans = "execution-plans"
    val ExecutionEvents = "execution-events"
    val Status = "status"
  }

  object SplineHttpHeaders {
    val AcceptRequestEncoding = "ABSA-Spline-Accept-Request-Encoding"
  }

  object Encoding {
    val GZIP = "gzip"
  }

}

/**
 * HttpLineageDispatcher is responsible for sending the lineage data to spline gateway through producer API
 *
 * @param splineServerRESTEndpointBaseURL spline producer API url
 * @param baseHttp                        http client
 * @param connectionTimeout               timeout for establishing TCP connection
 * @param readTimeout                     timeout for each individual TCP packet (in already established connection)
 */
class HttpLineageDispatcher(
  splineServerRESTEndpointBaseURL: String,
  baseHttp: BaseHttp,
  connectionTimeout: Duration,
  readTimeout: Duration)
  extends LineageDispatcher
    with Logging {

  def this(splineServerRESTEndpointBaseURL: String, http: BaseHttp) = this(
    splineServerRESTEndpointBaseURL: String,
    http: BaseHttp,
    DefaultConnectionTimeout,
    DefaultReadTimeout
  )

  def this(configuration: Configuration) = this(
    configuration.getRequiredString(ProducerUrlProperty),
    Http,
    configuration.getLong(ConnectionTimeoutMsKey, DefaultConnectionTimeout.toMillis).millis,
    configuration.getLong(ReadTimeoutMsKey, DefaultReadTimeout.toMillis).millis
  )

  log.info(s"spline.producer.url is set to:'$splineServerRESTEndpointBaseURL'")
  log.info(s"spline.timeout.connection is set to:'$connectionTimeout' ms")
  log.info(s"spline.timeout.read is set to:'$readTimeout' ms")

  private val executionPlansUrl = s"$splineServerRESTEndpointBaseURL/${RESTResource.ExecutionPlans}"
  private val executionEventsUrl = s"$splineServerRESTEndpointBaseURL/${RESTResource.ExecutionEvents}"
  private val statusUrl = s"$splineServerRESTEndpointBaseURL/${RESTResource.Status}"

  private val serverHeaders: Map[String, IndexedSeq[String]] = {
    val unableToConnectMsg = "Spark Agent was not able to establish connection to Spline Gateway"
    val serverHasIssuesMsg = "Connection to Spline Gateway: OK, but the Gateway is not initialized properly! Check Gateway's logs."

    Try(baseHttp(statusUrl)
      .method(HttpMethod.HEAD)
      .asString)
      .map {
        case response if response.is2xx => response.headers
        case response if response.is5xx => throw new SplineInitializationException(serverHasIssuesMsg)
        case response => throw new SplineInitializationException(s"$unableToConnectMsg. Http Status: ${response.code}")
      }
      .recover {
        case e: SplineInitializationException => throw e
        case NonFatal(e) => throw new SplineInitializationException(unableToConnectMsg, e)
        case _ => throw new SplineInitializationException(unableToConnectMsg)
      }
      .get
  }

  private lazy val requestCompressionSupported: Boolean =
    serverHeaders
      .get(SplineHttpHeaders.AcceptRequestEncoding)
      .toSeq.flatten
      .exists(_.toLowerCase == Encoding.GZIP)

  private def compressData(json: String): Array[Byte] = {
    val bytes = json.getBytes("UTF-8")
    val byteStream = new ByteArrayOutputStream(bytes.length)
    using(new GZIPOutputStream(byteStream))(_.write(bytes))
    byteStream.toByteArray // byteStream doesn't need to be closed
  }

  private def sendJson(json: String, url: String) = {
    log.trace(s"sendJson $url : $json")

    val http =
      if (requestCompressionSupported) {
        baseHttp(url)
          .postData(compressData(json))
          .header(HttpHeaders.CONTENT_ENCODING, Encoding.GZIP)
      } else {
        baseHttp(url)
          .postData(json)
      }

    try {
      http
        .compress(true) // response compression
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .timeout(connectionTimeout.toMillis.toInt, readTimeout.toMillis.toInt)
        .asString
        .throwError
        .body
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Cannot send lineage data to $url", e)
    }
  }

  override def send(executionPlan: ExecutionPlan): String = {
    sendJson(executionPlan.toJson, executionPlansUrl)
  }

  override def send(event: ExecutionEvent): Unit = {
    sendJson(Seq(event).toJson, executionEventsUrl)
  }
}
