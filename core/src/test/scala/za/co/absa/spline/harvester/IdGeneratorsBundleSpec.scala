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

package za.co.absa.spline.harvester

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.harvester.IdGenerator.UUIDGeneratorFactory
import za.co.absa.spline.producer.model.ExecutionPlan

class IdGeneratorsBundleSpec
  extends AnyFlatSpec
    with Matchers
    with MockitoSugar {

  behavior of "dataTypeIdGenerator"

  it should "generate deterministic sequence of unique ID" in {

    val execPlanUUIDGeneratorFactoryMock = mock[UUIDGeneratorFactory[Any, ExecutionPlan]]

    val gen1 = new IdGeneratorsBundle(execPlanUUIDGeneratorFactoryMock).dataTypeIdGenerator
    val gen2 = new IdGeneratorsBundle(execPlanUUIDGeneratorFactoryMock).dataTypeIdGenerator

    val seq1 = (1 to 10).map(_ => gen1.nextId())
    val seq2 = (1 to 10).map(_ => gen2.nextId())

    seq1 shouldEqual seq2
    seq1.distinct.length shouldBe 10
  }

}
