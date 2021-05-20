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
import scalaj.http.{HttpRequest, HttpResponse}
import za.co.absa.commons.lang.ARM.using
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpConstants.Encoding
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.rest.RestEndpoint._
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.AwsSigner

import java.time.{LocalDateTime, ZoneId}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials}

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import javax.ws.rs.HttpMethod

class RestEndpoint(val request: HttpRequest, val awsCredentials: Map[String,String], val proxyHost: String, val proxyPort: Int) {
  val awsCredentialProvider = new AWSStaticCredentialsProvider(new BasicSessionCredentials(awsCredentials("AwsAccessKey"), awsCredentials("AwsSecretKey"), awsCredentials("AwsToken")))
  val service = "execute-api"
  val region = "eu-west-1"
  def clock(): LocalDateTime = LocalDateTime.now(ZoneId.of("UTC"))
  val signer = AwsSigner(awsCredentialProvider, region, service, clock)

  def head(): HttpResponse[String] = {

    var jsonRequest = request
      .method(HttpMethod.HEAD)

    val emptyPayload: Option[Array[Byte]] = None
    val splitUrl = request.url.split("/")
    val host = splitUrl(2)
    val uri = "/" + splitUrl.drop(3).mkString("/")

    val tmpMap:Map[String, String] = signer.getSignedHeaders(
      uri= uri,
      method= HttpMethod.HEAD,
      queryParams = request.params.toMap,
      headers= request.headers.toMap + ("host" -> host),
      payload= emptyPayload
    )

    jsonRequest = jsonRequest.headers(tmpMap)
    if (proxyHost != "" && proxyPort != -1) {
      jsonRequest = jsonRequest.proxy(proxyHost, proxyPort)
    }
    jsonRequest.asString
  }

  def post(data: String, contentType: String, enableRequestCompression: Boolean): HttpResponse[String] = {
    var jsonRequest = request
      .method(HttpMethod.POST)
      .header(HttpHeaders.CONTENT_TYPE, contentType)

    val splitUrl = request.url.split("/")
    val host = splitUrl(2)
    val uri = "/" + splitUrl.drop(3).mkString("/")
    var payloadValue = Option(data.getBytes("UTF-8"))

    if (enableRequestCompression && data.length > GzipCompressionLengthThreshold) {
      payloadValue = Option(gzipContent(data.getBytes("UTF-8")))
      jsonRequest = jsonRequest.header(HttpHeaders.CONTENT_ENCODING, Encoding.GZIP).postData(gzipContent(data.getBytes("UTF-8")))
    } else{
      jsonRequest = jsonRequest.postData(data)
    }

    if (proxyHost != "" && proxyPort != -1) {
      jsonRequest = jsonRequest.proxy(proxyHost, proxyPort)
    }

    val tmpMap:Map[String, String] = signer.getSignedHeaders(
      uri= uri,
      method= HttpMethod.POST,
      queryParams = request.params.toMap,
      headers= request.headers.toMap + ("host" -> host),
      payload= payloadValue
    )

    jsonRequest = jsonRequest
      .headers(tmpMap)

    jsonRequest.asString
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
