/*
 * Copyright 2022 ABSA Group Limited
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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

class FallbackLineageDispatcherSpec extends AnyFlatSpec with MockitoSugar {

  behavior of "FallbackLineageDispatcher"

  it should "not interact with fallback dispatcher when primary dispatcher works" in {
    val primaryMock = mock[LineageDispatcher]
    val fallbackMock = mock[LineageDispatcher]
    val planMock = mock[ExecutionPlan]
    val eventMock = mock[ExecutionEvent]

    val dispatcher = new FallbackLineageDispatcher(primaryMock, fallbackMock)
    dispatcher.send(planMock)
    dispatcher.send(eventMock)

    verifyNoInteractions(fallbackMock)
    verify(primaryMock).send(planMock)
    verify(primaryMock).send(eventMock)
  }

  it should "use fallback dispatcher when primary dispatcher fails" in {
    val primaryMock = mock[LineageDispatcher]
    val fallbackMock = mock[LineageDispatcher]
    val planMock = mock[ExecutionPlan]
    val eventMock = mock[ExecutionEvent]

    when(primaryMock.send(any[ExecutionEvent])) thenThrow new RuntimeException("for test")

    val dispatcher = new FallbackLineageDispatcher(primaryMock, fallbackMock)
    dispatcher.send(planMock)
    dispatcher.send(eventMock)

    verify(primaryMock).send(planMock)
    verify(fallbackMock).send(eventMock)
  }

}
