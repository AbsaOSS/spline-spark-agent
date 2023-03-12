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

import scalaj.http.{Base64, HttpRequest}

import scala.collection.mutable.Map
import scala.collection.immutable.Map
import org.apache.spark.internal.Logging

trait Authentication {
  def createRequest(httpRequest: HttpRequest,authentication: scala.collection.immutable.Map[String,String]):HttpRequest
}

case class NoAuthentication(authentication: scala.collection.immutable.Map[String, String]) extends Authentication with Logging {
  override def createRequest(httpRequest: HttpRequest, authentication: scala.collection.immutable.Map[String, String]): HttpRequest = httpRequest
}
case class ClientCredentialsAuthentication(authentication: scala.collection.immutable.Map[String, String]) extends Authentication with Logging {
  case class Token(tokenValue: String, expirationTime: Instant)
  private val tokenCache: scala.collection.mutable.Map[String, Token] = scala.collection.mutable.Map.empty[String, Token]
  private val clientId: String = authentication.getOrElse("clientId","")
  private val clientSecret: String = authentication.getOrElse("clientSecret","")
  private val tokenUrl: String = authentication.getOrElse("tokenUrl","")
  private val grantType: String = authentication.getOrElse("grantType","")
  private val scope: String = authentication.getOrElse("scope","")

  private def isTokenExpired(expirationTime: Instant): Boolean = {
    Instant.now().isAfter(expirationTime.minusSeconds(300) )
  }

  private def getToken(clientId: String, clientSecret: String, tokenUrl: String,scope: String): String = {
    val cachedToken= tokenCache.get("token")
    val failureMessage = "Failed to retrieve token from response"
    if (cachedToken.isDefined && !isTokenExpired(cachedToken.getOrElse(throw new IllegalArgumentException(failureMessage)).expirationTime)) {
      // Token found in cache and not expired, return it
      cachedToken.getOrElse(throw new IllegalArgumentException(failureMessage)).tokenValue
    } else {
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
            val expirationTime = Instant.now().plus(Duration.ofSeconds(map.getOrElse("expires_in", "0").toString.toLong))
            val newToken = Token(token,expirationTime)
            tokenCache.put("token",newToken)
          }
          logInfo(token)
          token
        case _ =>
          throw new RuntimeException(failureMessage)

      }
    }
  }

  override def createRequest(httpRequest: HttpRequest, authentication: scala.collection.immutable.Map[String, String]): HttpRequest ={
    authentication.get("mode") match {
      case Some("enabled") => {
        if (authentication.contains("clientId") && authentication.contains("clientSecret") && authentication.contains("tokenUrl") && authentication.contains("scope")) {
          val token = getToken(clientId, clientSecret, tokenUrl, scope)
          httpRequest.header("Authorization", s"Bearer $token")
        } else {
          throw new IllegalArgumentException("Missing or wrong credentials")
        }
      }
      case _ => httpRequest
    }
  }
}

object AuthenticationFactory extends Logging {
  def createAuthentication(authentication: scala.collection.immutable.Map[String, String]): Authentication = {
    logInfo(authentication("grantType"))
    authentication("grantType") match {
      case "client_credentials" => ClientCredentialsAuthentication(authentication)
      case _ => NoAuthentication(authentication)
    }
  }
}

