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

import java.time.{Duration, Instant}

import scalaj.http.HttpRequest

import org.apache.spark.internal.Logging

trait Authentication {
  def createRequest(httpRequest: HttpRequest, authentication: scala.collection.immutable.Map[String, String]): HttpRequest
}

case class NoAuthentication(authentication: scala.collection.immutable.Map[String, String]) extends Authentication {
  override def createRequest(httpRequest: HttpRequest, authentication: scala.collection.immutable.Map[String, String]): HttpRequest = httpRequest
}

case class ClientCredentialsAuthentication(authentication: scala.collection.immutable.Map[String, String]) extends Authentication with Logging {

  val failureMessage = "Failed to retrieve token from response"
  private val clientId: String = authentication("clientId")
  private val clientSecret: String = authentication("clientSecret")
  private val tokenUrl: String = authentication("tokenUrl")
  private val grantType: String = authentication("grantType")
  private val scope: String = authentication("scope")
  private var tokenCache: Option[Token] = None

  override def createRequest(httpRequest: HttpRequest, authentication: scala.collection.immutable.Map[String, String]): HttpRequest = {
    val token = getToken
    httpRequest.header("Authorization", s"Bearer $token")
  }

  private def getToken: String = {
    if (isTokenInvalid) {
      val resp = scalaj.http.Http(tokenUrl)
        .postForm(Seq(
          "grant_type" -> grantType,
          "client_id" -> clientId,
          "client_secret" -> clientSecret,
          "scope" -> scope))
        .asString

      val jsonResp = scala.util.parsing.json.JSON.parseFull(resp.body)
      jsonResp match {
        case Some(map: scala.collection.immutable.Map[String, Any]) =>
          val token = map.getOrElse("access_token", "").toString
          if (token.nonEmpty) {
            val expirationTime = Instant.now().plus(Duration.ofSeconds(map.getOrElse("expires_in", 0).toString.toLong))
            val newToken = Token(token, expirationTime)
            tokenCache = Some(newToken)
          }
          token
        case _ =>
          throw new RuntimeException(failureMessage)
      }
    } else {
      tokenCache.getOrElse(throw new RuntimeException(failureMessage)).tokenValue
    }
  }

  private def isTokenInvalid: Boolean = {
    tokenCache.isEmpty || Instant.now().isAfter(tokenCache.get.expirationTime.minusSeconds(300))
  }

  case class Token(tokenValue: String, expirationTime: Instant)
}


object AuthenticationFactory extends Logging {
  def createAuthentication(authentication: scala.collection.immutable.Map[String, String]): Authentication = {
    if (authentication.isDefinedAt("mode") && authentication.isDefinedAt("grantType")) {
      authentication("mode") match {
        case "enabled" => authentication("grantType") match {
          case "client_credentials" => ClientCredentialsAuthentication(authentication)
          case _ =>
            throw new IllegalArgumentException(s"$authentication('grantType') not implemented")
        }
        case _ =>
          logInfo("Authentication mode is set to Disabled")
          NoAuthentication(authentication)
      }
    }
    else {
      logInfo("Authentication mode is set to Disabled")
      NoAuthentication(authentication)
    }
  }
}

