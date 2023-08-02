/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.MapConfiguration
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{anyBoolean, anyString}
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import scalaj.http._
import za.co.absa.spline.commons.version.Version.VersionStringInterpolator
import za.co.absa.spline.harvester.conf.AuthenticationType
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.RESTResource
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.auth.Authentication
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.rest.{RestClient, RestEndpoint}
import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.producer.model.ExecutionEvent

import java.util.UUID
import javax.ws.rs.core.MediaType
import scala.collection.JavaConverters._

class HttpLineageDispatcherSpec extends AnyFlatSpec with MockitoSugar {

  behavior of "HttpLineageDispatcher"

  private val restClientMock = mock[RestClient]
  private val httpRequestMock = mock[HttpRequest]
  private val httpResponseMock = mock[HttpResponse[String]]
  private val authConfig = new MapConfiguration(Map(Authentication.ConfProps.Type -> AuthenticationType.NONE.name).asJava)
  private val restEndpointMock = new RestEndpoint(httpRequestMock, authConfig)
  when(restClientMock.endpoint("status")) thenReturn restEndpointMock
  when(httpRequestMock.method("HEAD")) thenReturn httpRequestMock

  it should "not do anything when producer is ready" in {
    when(httpRequestMock.asString) thenReturn httpResponseMock
    when(httpResponseMock.is2xx) thenReturn true
    when(httpResponseMock.headers) thenReturn Map.empty[String, IndexedSeq[String]]

    new HttpLineageDispatcher(restClientMock, None, None)
  }

  it should "throw when producer is not ready" in {
    when(httpRequestMock.asString) thenReturn httpResponseMock
    when(httpResponseMock.is2xx) thenReturn false
    when(httpResponseMock.is5xx) thenReturn true

    assertThrows[SplineInitializationException] {
      new HttpLineageDispatcher(restClientMock, None, None)
    }
  }

  it should "throw when connection to producer was not successful" in {
    when(httpRequestMock.asString) thenThrow new RuntimeException

    assertThrows[SplineInitializationException] {
      new HttpLineageDispatcher(restClientMock, None, None)
    }
  }

  it should "use version and compressions local configuration when provided" in {
    val restClientMock = mock[RestClient]

    val requestMock = mock[HttpRequest]
    when(requestMock.url) thenReturn "someUrl"

    val statusEndpointMock = mock[RestEndpoint]
    when(restClientMock.endpoint(RESTResource.Status)) thenReturn statusEndpointMock
    when(statusEndpointMock.request) thenReturn requestMock
    val responseHeaders = Map(
      SplineHeaders.ApiVersion -> IndexedSeq("1.1"),
      SplineHeaders.AcceptRequestEncoding -> IndexedSeq("gzip")
    )
    when(statusEndpointMock.head()) thenReturn HttpResponse("", 200, responseHeaders)


    val eventsEndpointMock = mock[RestEndpoint]
    when(restClientMock.endpoint(RESTResource.ExecutionEvents)) thenReturn eventsEndpointMock
    when(eventsEndpointMock.request) thenReturn requestMock
    when(eventsEndpointMock.post(anyString(), anyString(), anyBoolean())) thenReturn HttpResponse("", 200, Map.empty)

    val dispatcher = new HttpLineageDispatcher(restClientMock, Some(ver"1"), Some(false))

    dispatcher.send(ExecutionEvent(UUID.randomUUID(), Map.empty, 1L, None, None, None, Map.empty))

    verify(eventsEndpointMock).post(
      anyString(),
      ArgumentMatchers.eq(MediaType.APPLICATION_JSON),
      ArgumentMatchers.eq(false)
    )
  }

  it should "get version and compressions from server by default" in {
    val restClientMock = mock[RestClient]

    val requestMock = mock[HttpRequest]
    when(requestMock.url) thenReturn "someUrl"

    val statusEndpointMock = mock[RestEndpoint]
    when(restClientMock.endpoint(RESTResource.Status)) thenReturn statusEndpointMock
    when(statusEndpointMock.request) thenReturn requestMock
    val responseHeaders = Map(
      SplineHeaders.ApiVersion -> IndexedSeq("1.1"),
      SplineHeaders.AcceptRequestEncoding -> IndexedSeq("gzip")
    )
    when(statusEndpointMock.head()) thenReturn HttpResponse("", 200, responseHeaders)


    val eventsEndpointMock = mock[RestEndpoint]
    when(restClientMock.endpoint(RESTResource.ExecutionEvents)) thenReturn eventsEndpointMock
    when(eventsEndpointMock.request) thenReturn requestMock
    when(eventsEndpointMock.post(anyString(), anyString(), anyBoolean())) thenReturn HttpResponse("", 200, Map.empty)

    val dispatcher = new HttpLineageDispatcher(restClientMock, None, None)

    dispatcher.send(ExecutionEvent(UUID.randomUUID(), Map.empty, 1L, None, None, None, Map.empty))

    verify(eventsEndpointMock).post(
      anyString(),
      ArgumentMatchers.eq("application/vnd.absa.spline.producer.v1.1+json"),
      ArgumentMatchers.eq(true)
    )
  }

  it should "use API version and compression from the properties if they provided, and do not contact server" in {
    val restClientMock = mock[RestClient]

    val requestMock = mock[HttpRequest]
    when(requestMock.url) thenReturn "someUrl"

    val eventsEndpointMock = mock[RestEndpoint]
    when(restClientMock.endpoint(RESTResource.ExecutionEvents)) thenReturn eventsEndpointMock
    when(eventsEndpointMock.request) thenReturn requestMock
    when(eventsEndpointMock.post(anyString(), anyString(), anyBoolean())) thenReturn HttpResponse("", 200, Map.empty)

    val dispatcher = new HttpLineageDispatcher(restClientMock, Some(ProducerApiVersion.V1_1), Some(true))

    dispatcher.send(ExecutionEvent(UUID.randomUUID(), Map.empty, 1L, None, None, None, Map.empty))

    verify(eventsEndpointMock).post(
      anyString(),
      ArgumentMatchers.eq("application/vnd.absa.spline.producer.v1.1+json"),
      ArgumentMatchers.eq(true)
    )
  }
}
