/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher.modelmapper

import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class ModelMapperV11Spec
  extends AnyFlatSpec
    with Matchers
    with MockitoSugar {

  private val mapper = ModelMapperV11

  behavior of "toDTO()"

  it should "convert ExecutionPlan model from the current version to DTO ver 1.1" in {
    val mockPlan = mock[mapper.TPlan]
    mapper.toDTO(mockPlan) shouldEqual Some(mockPlan)
  }

  it should "convert ExecutionEvent model from the current version to DTO ver 1.1" in {
    val mockEvent = mock[mapper.TEvent]
    when(mockEvent.error) thenReturn None
    mapper.toDTO(mockEvent) shouldEqual Some(mockEvent)
  }

  it should "skip failed events" in {
    val mockEvent = mock[mapper.TEvent]
    when(mockEvent.error) thenReturn Some("dummy error")
    mapper.toDTO(mockEvent) shouldEqual None
  }
}
