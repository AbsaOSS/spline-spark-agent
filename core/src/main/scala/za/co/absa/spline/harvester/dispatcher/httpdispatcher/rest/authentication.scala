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

import org.apache.spark.internal.Logging
import scalaj.http.{Http, HttpRequest}
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._

import java.time.{Duration, Instant}


trait Authentication {
  def authenticate(httpRequest: HttpRequest, authParams: Map[String, String]): HttpRequest
}

object NoAuthentication extends Authentication {
  override def authenticate(httpRequest: HttpRequest, authParams: Map[String, String]): HttpRequest = httpRequest
}

class ClientCredentialsAuthentication(authentication: Map[String, String]) extends Authentication with Logging {
  private val clientId: String = authentication("clientId")
  private val clientSecret: String = authentication("clientSecret")
  private val tokenUrl: String = authentication("tokenUrl")
  private val grantType: String = authentication("grantType")
  private val scope: String = authentication("scope")

  private var tokenCache: Option[Token] = None

  override def authenticate(httpRequest: HttpRequest, authParams: Map[String, String]): HttpRequest = {
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
        "client_secret" -> clientSecret,
        "scope" -> scope))
      .asString

    val jsonResp = resp.body.fromJson[Map[String, Any]]
    val freshToken = jsonResp.getOrElse("access_token", sys.error("Failed to retrieve token from response")).toString
    val expirationTime = Instant.now().plus(Duration.ofSeconds(jsonResp.getOrElse("expires_in", 0).toString.toLong))

    Token(freshToken, expirationTime)
  }
}

private case class Token(tokenValue: String, expirationTime: Instant)

object AuthenticationFactory extends Logging {
  def createAuthentication(authentication: Map[String, String]): Authentication = {
    val maybeAuth =
      for {
        mode <- authentication.get("mode") if mode == "enabled"
        grantType <- authentication.get("grantType")
      } yield grantType match {
        case "client_credentials" => new ClientCredentialsAuthentication(authentication)
        case _ => throw new IllegalArgumentException(s"$grantType not implemented")
      }

    maybeAuth.getOrElse {
      logInfo("Authentication mode is set to Disabled")
      NoAuthentication
    }
  }
}

