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

package za.co.absa.spline.harvester.dispatcher.httpdispatcher.rest

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import scalaj.http.HttpOptions.HttpOption
import scalaj.http.{BaseHttp, HttpOptions}
import za.co.absa.spline.commons.lang.extensions.AnyExtension._
import za.co.absa.spline.harvester.dispatcher.SplineHeaders

import scala.concurrent.duration.Duration

trait RestClient {
  def endpoint(url: String): RestEndpoint
}

object RestClient extends Logging {
  /**
   * @param baseHttp    HTTP client
   * @param baseURL     REST endpoint base URL
   * @param connTimeout timeout for establishing TCP connection
   * @param readTimeout timeout for each individual TCP packet (in already established connection)
   */
  def apply(
    baseHttp: BaseHttp,
    baseURL: String,
    connTimeout: Duration,
    readTimeout: Duration,
    disableSslValidation: Boolean,
    headers: Map[String, String],
    authConfig: Configuration
  ): RestClient = {

    logDebug(s"baseURL = $baseURL")
    logDebug(s"connTimeout = $connTimeout")
    logDebug(s"readTimeout = $readTimeout")
    logDebug(s"disableSslValidation = $disableSslValidation")
    logDebug(s"headers = $headers")

    val maybeDisableSslValidationOption: Option[HttpOption] =
      if (disableSslValidation) {
        logWarning(s"SSL validation is DISABLED -- not recommended for production!")
        Some(HttpOptions.allowUnsafeSSL)
      } else {
        None
      }

    //noinspection ConvertExpressionToSAM
    new RestClient {
      override def endpoint(resource: String): RestEndpoint = new RestEndpoint(
        baseHttp(s"$baseURL/$resource")
          .option(HttpOptions.connTimeout(connTimeout.toMillis.toInt))
          .option(HttpOptions.readTimeout(readTimeout.toMillis.toInt))
          .having(maybeDisableSslValidationOption)(_ option _)
          .header(SplineHeaders.Timeout, readTimeout.toMillis.toString)
          .headers(headers)
          .compress(true),
        authConfig)
    }
  }
}
