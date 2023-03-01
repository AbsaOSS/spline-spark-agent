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

import org.apache.http.HttpHeaders
import scalaj.http.{ HttpRequest, HttpResponse }
import za.co.absa.commons.lang.ARM.using
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpConstants.Encoding
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.rest.RestEndpoint._

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import javax.ws.rs.HttpMethod
import java.time.{ Instant, Duration }
import scala.collection.mutable

class RestEndpoint(val request: HttpRequest, val authentication: Map[String, String]) {

  private val myCache: mutable.Map[String, (String, Instant)] = mutable.Map.empty[String, (String, Instant)]

  def withAuth(): HttpRequest = {
    if (authentication.contains("clientId") && authentication.contains("clientSecret") && authentication.contains("tokenUrl") && authentication.contains("scope")) {
      val clientId = authentication("clientId")
      val clientSecret = authentication("clientSecret")
      val tokenUrl = authentication("tokenUrl")
      val token = getToken(clientId, clientSecret, tokenUrl)
      request.header("Authorization", s"Bearer $token")
    } else {
      request
    }
  }

  private def getToken(clientId: String, clientSecret: String, tokenUrl: String): String = {
    val cachedToken = myCache.get("token")
    if (cachedToken.isDefined && !isTokenExpired(cachedToken.get._2)) {
      // Token found in cache and not expired, return it
      cachedToken.get._1
    } else {
      val resp = scalaj.http.Http(tokenUrl)
        .postForm(Seq(
          "grant_type" -> "client_credentials",
          "client_id" -> clientId,
          "client_secret" -> clientSecret))
        .asString

      val jsonResp = scala.util.parsing.json.JSON.parseFull(resp.body)
      jsonResp match {
        case Some(map: Map[String, Any]) =>
          val token = map.getOrElse("access_token", "").toString
          if (token.nonEmpty) {
            val expirationTime = Instant.now().plus(Duration.ofSeconds(map.getOrElse("expires_in", "0").toString.toLong))
            myCache.put("token", (token, expirationTime))
          }
          token
        case _ =>
          throw new RuntimeException("Failed to retrieve token from response")

      }
    }
  }

  private def isTokenExpired(expirationTime: Instant): Boolean = {
    Instant.now().isAfter(expirationTime)
  }

  def head(): HttpResponse[String] = withAuth()
    .method(HttpMethod.HEAD)
    .asString

  def post(data: String, contentType: String, enableRequestCompression: Boolean): HttpResponse[String] = {
    val jsonRequest = withAuth()
      .header(HttpHeaders.CONTENT_TYPE, contentType)

    if (enableRequestCompression && data.length > GzipCompressionLengthThreshold) {
      jsonRequest
        .header(HttpHeaders.CONTENT_ENCODING, Encoding.GZIP)
        .postData(gzipContent(data.getBytes("UTF-8")))
        .asString
    } else {
      jsonRequest
        .postData(data)
        .asString
    }
  }
}

object RestEndpoint {
  private val GzipCompressionLengthThreshold: Int = 2048

  private def gzipContent(bytes: Array[Byte]): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream(bytes.length)
    using(new GZIPOutputStream(byteStream))(_.write(bytes))
    byteStream.toByteArray
  }
}
