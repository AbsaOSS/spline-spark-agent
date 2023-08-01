/*
 * Copyright 2023 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher.httpdispatcher.auth

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import scalaj.http.HttpRequest
import za.co.absa.spline.commons.ConfigurationImplicits.ConfOps
import za.co.absa.spline.harvester.conf.AuthenticationType

trait Authentication {
  def authenticate(httpRequest: HttpRequest, authConfig: Configuration): HttpRequest
}

object Authentication extends Logging {

  object ConfProps {
    val Type = "type"
  }

  def fromConfig(authConfig: Configuration): Authentication = {
    val authType = authConfig.getRequiredEnum[AuthenticationType](ConfProps.Type)
    authType match {
      case AuthenticationType.NONE =>
        logInfo("Authentication is disabled")
        NoAuthentication
      case AuthenticationType.OAUTH =>
        logInfo("Authentication type: OAUTH")
        new OAuthAuthentication(authConfig)
      case unknownType =>
        throw new IllegalArgumentException(s"$unknownType authentication is not implemented")
    }
  }
}

