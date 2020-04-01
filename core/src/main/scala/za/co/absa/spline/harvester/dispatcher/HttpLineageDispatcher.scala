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

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import scalaj.http.{BaseHttp, Http}
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.dispatcher.HttpLineageDispatcher.RESTResource
import za.co.absa.spline.harvester.dispatcher.HttpLineageDispatcher._
import za.co.absa.spline.harvester.exception.SplineNotInitializedException
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}
import za.co.absa.commons.lang.ARM.{managed, using}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object HttpLineageDispatcher {
  val producerUrlProperty = "spline.producer.url"
  val connectionTimeoutMsKey = "spline.timeout.connection"
  val readTimeoutMsKey = "spline.timeout.read"

  val defaultConnectionTimeout = 1000
  val defaultReadTimeout = 20000

  object RESTResource {
    val ExecutionPlans = "execution-plans"
    val ExecutionEvents = "execution-events"
    val Status = "status"
  }
}

/**
 * HttpLineageDispatcher is responsible for sending the lineage data to spline gateway through producer API
 *
 * @param splineServerRESTEndpointBaseURL spline producer API url
 * @param baseHttp http client
 * @param connectionTimeoutMs timeout for establishing TCP connection
 * @param readTimeoutMs timeout for each individual TCP packet (in already established connection)
 */
class HttpLineageDispatcher(
     splineServerRESTEndpointBaseURL: String,
     baseHttp: BaseHttp,
     connectionTimeoutMs: Int,
     readTimeoutMs: Int)
  extends LineageDispatcher
  with Logging {

  def this(splineServerRESTEndpointBaseURL: String, http: BaseHttp) = this(
    splineServerRESTEndpointBaseURL: String,
    http: BaseHttp,
    defaultConnectionTimeout,
    defaultReadTimeout
  )

  def this(configuration: Configuration) = this(
    configuration.getRequiredString(producerUrlProperty),
    Http,
    configuration.getInt(connectionTimeoutMsKey, defaultConnectionTimeout),
    configuration.getInt(readTimeoutMsKey, defaultReadTimeout)
  )

  log.info(s"spline.producer.url is set to:'${splineServerRESTEndpointBaseURL}'")
  log.info(s"spline.timeout.connection is set to:'${connectionTimeoutMs}' ms")
  log.info(s"spline.timeout.read is set to:'${readTimeoutMs}' ms")

  val executionPlansUrl = s"$splineServerRESTEndpointBaseURL/${RESTResource.ExecutionPlans}"
  val executionEventsUrl = s"$splineServerRESTEndpointBaseURL/${RESTResource.ExecutionEvents}"
  val statusUrl = s"$splineServerRESTEndpointBaseURL/${RESTResource.Status}"

  private lazy val useRequestCompression = isServerCompressionReady()

  override def send(executionPlan: ExecutionPlan): String = {
    sendJson(executionPlan.toJson, executionPlansUrl)
  }

  override def send(event: ExecutionEvent): Unit = {
    sendJson(Seq(event).toJson, executionEventsUrl)
  }

  private def sendJson(json: String, url: String) = {
    log.debug(s"sendJson $url : $json")

    try {
      val http = if (useRequestCompression) {
        baseHttp(url)
          .postData(compressData(json))
          .header("content-encoding", "gzip")
      } else {
        baseHttp(url)
          .postData(json)
      }

      http
      .compress(true) // response compression
      .header("content-type", "application/json")
      .timeout(connectionTimeoutMs, readTimeoutMs)
      .asString
      .throwError
      .body

    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Cannot send lineage data to $url", e)
    }
  }

  private def compressData(json: String): Array[Byte] = {

    val bytes = json.getBytes("UTF-8")

    val byteStream = new ByteArrayOutputStream(bytes.length)
    using(new GZIPOutputStream(byteStream))(_.write(bytes))
    byteStream.toByteArray // byteStream doesn't need to be closed
  }

  override def ensureProducerReady(): Unit = {
    val tryStatusOk = Try(baseHttp(statusUrl)
      .method("HEAD")
      .asString
      .isSuccess)

    tryStatusOk match {
      case Success(false) => throw new SplineNotInitializedException(
        "Connection to Spline Gateway: OK, but the Gateway is not initialized properly! Check Gateway's logs.")
      case Failure(e) if NonFatal(e) => throw new SplineNotInitializedException(
        "Spark Agent was not able to establish connection to Spline Gateway.", e)
      case _ => Unit
    }
  }

  private def isServerCompressionReady(): Boolean = {
    val tryStatus = Try(baseHttp(statusUrl)
      .method("GET")
      .asString
      .throwError
      .body)

    tryStatus
      .toOption
      .map(extractCompressionFlag(_))
      .getOrElse(false)
  }

  private def extractCompressionFlag(json: String): Boolean = {

    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val jsonAST = parse(json)

    val compressList = for {
      JObject(child) <- jsonAST
      JField("requestDecompression", JBool(compress))  <- child
    } yield compress

    compressList.head
  }

}
