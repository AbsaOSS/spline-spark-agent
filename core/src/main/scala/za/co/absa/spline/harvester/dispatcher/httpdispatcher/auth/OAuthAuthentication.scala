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
import scalaj.http.{Http, HttpRequest}
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.auth.OAuthAuthentication.{ConfProps, GrantTypes, Token}
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._

import java.time.{Duration, Instant}

class OAuthAuthentication(authConfig: Configuration) extends Authentication with Logging {
  private val tokenUrl: String = authConfig.getRequiredString(ConfProps.TokenUrl)
  private val grantType: String = authConfig.getRequiredString(ConfProps.GrantType)

  require(grantType == GrantTypes.ClientCredentials, "Only 'client_credentials' grant type is currently supported")

  private val clientId: String = authConfig.getRequiredString(ConfProps.ClientId)
  private val clientSecret: String = authConfig.getRequiredString(ConfProps.ClientSecret)
  private val maybeScope: Option[String] = authConfig.getOptionalString(ConfProps.Scope)

  private var tokenCache: Option[Token] = None

  override def authenticate(httpRequest: HttpRequest, authConfig: Configuration): HttpRequest = {
    val token = tokenCache
      .filter(_.expirationTime.minusSeconds(300) isAfter Instant.now())
      .getOrElse(obtainFreshToken())

    tokenCache = Some(token)

    httpRequest.header("Authorization", s"Bearer ${token.tokenValue}")
  }

  private def obtainFreshToken() = {
    val resp = Http(tokenUrl)
      .postForm(Seq(
        "grant_type" -> grantType,
        "client_id" -> clientId,
        "client_secret" -> clientSecret
      ) ++ maybeScope.map("scope" -> _))
      .asString

    val jsonResp = resp.body.fromJson[Map[String, Any]]
    val freshToken = jsonResp.getOrElse("access_token", sys.error("Failed to retrieve token from response")).toString
    val expirationTime = Instant.now().plus(Duration.ofSeconds(jsonResp.getOrElse("expires_in", 0).toString.toLong))

    Token(freshToken, expirationTime)
  }
}

object OAuthAuthentication {
  private case class Token(tokenValue: String, expirationTime: Instant)

  object ConfProps {
    val TokenUrl = "tokenUrl"
    val GrantType = "grantType"
    val ClientId = "clientId"
    val ClientSecret = "clientSecret"
    val Scope = "scope"
  }

  object GrantTypes {
    val ClientCredentials = "client_credentials"
  }
}
