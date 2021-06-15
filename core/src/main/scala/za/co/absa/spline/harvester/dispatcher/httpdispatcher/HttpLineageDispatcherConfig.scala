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
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpLineageDispatcherConfig._

import scala.concurrent.duration._

object HttpLineageDispatcherConfig {
  val ProducerUrlProperty = "producer.url"
  val ConnectionTimeoutMsKey = "timeout.connection"
  val ReadTimeoutMsKey = "timeout.read"

  val AwsAccessKeyProperty = "awsAccessKey"
  val AwsSecretKeyProperty = "awsSecretKey"
  val AwsTokenProperty = "awsToken"
  val AwsRegionProperty = "awsRegion"

  val proxyHostProperty = "proxyHost"
  val proxyPortProperty = "proxyPort"

  val DefaultConnectionTimeout: Duration = 1.second
  val DefaultReadTimeout: Duration = 20.second

  def apply(c: Configuration) = new HttpLineageDispatcherConfig(c)
}

class HttpLineageDispatcherConfig(config: Configuration) {
  val producerUrl: String = config.getRequiredString(ProducerUrlProperty)

  val awsCredentials: Map[String,String] = Map(
    "AwsAccessKey" -> config.getRequiredString(AwsAccessKeyProperty),
    "AwsSecretKey" -> config.getRequiredString(AwsSecretKeyProperty),
    "AwsToken" -> config.getRequiredString(AwsTokenProperty)
  )

  val awsRegion: String = config.getRequiredString(AwsRegionProperty)

  val proxyHost: String = config.getOptionalString(proxyHostProperty).getOrElse("")
  val proxyPort: Int = config.getOptionalInt(proxyPortProperty).getOrElse("-1".toInt)

  val connTimeout: Duration = config
    .getOptionalLong(ConnectionTimeoutMsKey)
    .map(_.millis)
    .getOrElse(DefaultConnectionTimeout)

  val readTimeout: Duration = config
    .getOptionalLong(ReadTimeoutMsKey)
    .map(_.millis)
    .getOrElse(DefaultReadTimeout)
}
