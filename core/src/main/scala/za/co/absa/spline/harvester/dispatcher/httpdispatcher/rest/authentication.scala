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
import scalaj.http.{Http, HttpRequest}
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._

import java.time.{Duration, Instant}


trait Authentication {
  def authenticate(httpRequest: HttpRequest, authConfig: Configuration): HttpRequest
}

object NoAuthentication extends Authentication {
  override def authenticate(httpRequest: HttpRequest, authConfig: Configuration): HttpRequest = httpRequest
}

class OAuthAuthentication(authentication: Configuration) extends Authentication with Logging {
  private val tokenUrl: String = authentication.getRequiredString("tokenUrl")
  private val grantType: String = authentication.getRequiredString("grantType")
  private val clientId: String = authentication.getRequiredString("clientId")
  private val clientSecret: String = authentication.getRequiredString("clientSecret")
  private val maybeScope: Option[String] = authentication.getOptionalString("scope")

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

private case class Token(tokenValue: String, expirationTime: Instant)

object AuthenticationFactory extends Logging {
  def createAuthentication(authConfig: Configuration): Authentication = {
    val maybeAuth =
      for {
        mode <- authConfig.getOptionalString("mode") if mode == "enabled"
        grantType <- authConfig.getOptionalString("grantType")
      } yield grantType match {
        case "client_credentials" => new OAuthAuthentication(authConfig)
        case _ => throw new IllegalArgumentException(s"$grantType not implemented")
      }

    maybeAuth.getOrElse {
      logInfo("Authentication mode is set to Disabled")
      NoAuthentication
    }
  }
}

