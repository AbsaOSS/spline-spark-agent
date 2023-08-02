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

package za.co.absa.spline.harvester.dispatcher.httpdispatcher

import org.apache.commons.configuration.Configuration
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.ProducerApiVersion
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpLineageDispatcherConfig._

import scala.concurrent.duration._

object HttpLineageDispatcherConfig {
  val AuthenticationProperty = "authentication"
  val ProducerUrlProperty = "producer.url"
  val ConnectionTimeoutMsKey = "timeout.connection"
  val ReadTimeoutMsKey = "timeout.read"
  val DisableSslValidation = "disableSslValidation"
  val ApiVersion = "apiVersion"
  val RequestCompression = "requestCompression"
  val Header = "header"

  def apply(c: Configuration) = new HttpLineageDispatcherConfig(c)
}

class HttpLineageDispatcherConfig(config: Configuration) {
  val producerUrl: String = config.getRequiredString(ProducerUrlProperty)
  val connTimeout: Duration = config.getRequiredLong(ConnectionTimeoutMsKey).millis
  val readTimeout: Duration = config.getRequiredLong(ReadTimeoutMsKey).millis
  val disableSslValidation: Boolean = config.getRequiredBoolean(DisableSslValidation)
  val headers: Map[String, String] = config.subset(Header).toMap[String]
  val authConfig: Configuration = config.subset(AuthenticationProperty)

  def apiVersionOption: Option[Version] = config.getOptionalString(ApiVersion).map(stringToVersion)

  def requestCompressionOption: Option[Boolean] = config.getOptionalBoolean(RequestCompression)

  private def stringToVersion(str: String): Version = str.trim match {
    case "LATEST" => ProducerApiVersion.SupportedApiRange.Max
    case s => Version.asSimple(s)
  }

}
