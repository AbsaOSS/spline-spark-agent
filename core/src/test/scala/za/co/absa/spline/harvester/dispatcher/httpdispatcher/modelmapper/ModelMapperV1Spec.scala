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

package za.co.absa.spline.harvester.dispatcher.httpdispatcher.modelmapper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.producer.model.{v1_0, v1_1}

import java.util.UUID

class ModelMapperV1Spec
  extends AnyFlatSpec
    with Matchers {

  behavior of "toDTO()"

  it should "convert ExecutionPlan model from the current version to ver 1.0" in {

    val dummyDataType = new Object()

    val planCur = v1_1.ExecutionPlan(
      id = Some(UUID.fromString("00000000-0000-0000-0000-000000000000")),
      name = Some("Foo Plan"),
      discriminator = None,
      operations = v1_1.Operations(
        write = v1_1.WriteOperation(
          outputSource = "aaa",
          append = true,
          id = "op-0",
          name = Some("Write Operation"),
          childIds = Seq("op-1"),
          params = Some(Map("param1" -> 42)),
          extra = Some(Map("extra1" -> 42))
        ),
        reads = Some(Seq(v1_1.ReadOperation(
          inputSources = Seq("bbb"),
          id = "op-2",
          name = Some("Read Operation"),
          output = Some(Seq("attr-1", "attr-2")),
          params = Some(Map("param2" -> 42)),
          extra = Some(Map("extra2" -> 42))
        ))),
        other = Some(Seq(v1_1.DataOperation(
          id = "op-1",
          name = Some("Data Operation"),
          childIds = Some(Seq("op-2")),
          output = Some(Seq("attr-3")),
          params = Some(Map("param3" -> 42)),
          extra = Some(Map("extra3" -> 42))
        )))
      ),
      attributes = Some(Seq(
        v1_1.Attribute(
          id = "attr-1",
          dataType = Some(dummyDataType),
          childRefs = None,
          extra = None,
          name = "A"
        ),
        v1_1.Attribute(
          id = "attr-2",
          dataType = Some(dummyDataType),
          childRefs = None,
          extra = None,
          name = "B"
        ),
        v1_1.Attribute(
          id = "attr-3",
          dataType = Some(dummyDataType),
          childRefs = None,
          extra = None,
          name = "C"
        )
      )),
      expressions = None,
      systemInfo = v1_1.NameAndVersion("xxx", "777"),
      agentInfo = Some(v1_1.NameAndVersion("yyy", "777")),
      extraInfo = Some(Map("extra42" -> 42))
    )

    val planV1 = ModelMapperV1.toDTO(planCur)

    planV1 shouldEqual v1_0.ExecutionPlan(
      id = Some(UUID.fromString("00000000-0000-0000-0000-000000000000")),
      operations = v1_0.Operations(
        write = v1_0.WriteOperation(
          outputSource = "aaa",
          append = true,
          id = 0,
          childIds = Seq(1),
          params = Some(Map("param1" -> 42)),
          extra = Some(Map(
            "extra1" -> 42,
            "name" -> "Write Operation"
          )),
          schema = None
        ),
        reads = Some(Seq(v1_0.ReadOperation(
          inputSources = Seq("bbb"),
          id = 2,
          childIds = Nil,
          schema = Some(Seq("attr-1", "attr-2")),
          params = Some(Map("param2" -> 42)),
          extra = Some(Map(
            "extra2" -> 42,
            "name" -> "Read Operation"
          ))
        ))),
        other = Some(Seq(v1_0.DataOperation(
          id = 1,
          childIds = Some(Seq(2)),
          schema = Some(Seq("attr-3")),
          params = Some(Map("param3" -> 42)),
          extra = Some(Map(
            "extra3" -> 42,
            "name" -> "Data Operation"
          ))
        )))
      ),
      systemInfo = v1_0.SystemInfo("xxx", "777"),
      agentInfo = Some(v1_0.AgentInfo("yyy", "777")),
      extraInfo = Some(Map(
        "extra42" -> 42,
        "appName" -> "Foo Plan",
        "attributes" -> Seq(
          Map("id" -> "attr-1", "name" -> "A", "dataTypeId" -> Some(dummyDataType)),
          Map("id" -> "attr-2", "name" -> "B", "dataTypeId" -> Some(dummyDataType)),
          Map("id" -> "attr-3", "name" -> "C", "dataTypeId" -> Some(dummyDataType))
        )))
    )
  }

  it should "convert ExecutionEvent model from the current version to ver 1.0" in {
    val eventCur = v1_1.ExecutionEvent(
      planId = UUID.fromString("00000000-0000-0000-0000-000000000000"),
      discriminator = None,
      timestamp = 123456789,
      durationNs = Some(555),
      error = None,
      extra = Some(Map("extra1" -> 42))
    )

    val eventV1 = ModelMapperV1.toDTO(eventCur)

    eventV1 shouldEqual v1_0.ExecutionEvent(
      planId = UUID.fromString("00000000-0000-0000-0000-000000000000"),
      timestamp = 123456789,
      error = None,
      extra = Some(Map(
        "extra1" -> 42,
        "durationNs" -> 555
      ))
    )
  }
}
