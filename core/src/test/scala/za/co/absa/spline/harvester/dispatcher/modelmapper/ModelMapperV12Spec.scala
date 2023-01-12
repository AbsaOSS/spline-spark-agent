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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.producer.dto.v1_2
import za.co.absa.spline.producer.model._

import java.util.UUID

class ModelMapperV12Spec
  extends AnyFlatSpec
    with Matchers
    with MockitoSugar {

  private val mapper = ModelMapperV12

  behavior of "toDTO()"

  it should "convert ExecutionPlan entity to DTO ver 1.2" in {
    val dummyDataType = new Object()

    val planEntity = ExecutionPlan(
      id = Some(UUID.fromString("00000000-0000-0000-0000-000000000000")),
      name = "Foo Plan",
      discriminator = None,
      labels = Map("lbl1" -> Seq("a", "b")),
      operations = Operations(
        write = WriteOperation(
          outputSource = "aaa",
          append = true,
          id = "op-0",
          name = "Write Operation",
          childIds = Seq("op-1"),
          params = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")),
          extra = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42"))
        ),
        reads = Seq(ReadOperation(
          inputSources = Seq("bbb"),
          id = "op-2",
          name = "Read Operation",
          output = Seq("attr-1", "attr-2"),
          params = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")),
          extra = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42"))
        )),
        other = Seq(DataOperation(
          id = "op-1",
          name = "Data Operation",
          childIds = Seq("op-2"),
          output = Seq("attr-3"),
          params = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")),
          extra = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42"))
        ))
      ),
      attributes = Seq(
        Attribute(
          id = "attr-1",
          dataType = Some(dummyDataType),
          childRefs = Seq.empty,
          extra = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")),
          name = "A"
        ),
        Attribute(
          id = "attr-2",
          dataType = Some(dummyDataType),
          childRefs = Seq.empty,
          extra = Map.empty,
          name = "B"
        ),
        Attribute(
          id = "attr-3",
          dataType = Some(dummyDataType),
          childRefs = Seq.empty,
          extra = Map.empty,
          name = "C"
        )
      ),
      expressions = Expressions(
        functions = Seq(
          FunctionalExpression(
            id = "e1",
            dataType = Some(dummyDataType),
            childRefs = Seq(AttrRef("a1")),
            extra = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")),
            name = "Expr1",
            params = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42"))
          )
        ),
        constants = Seq(
          Literal(
            id = "c1",
            dataType = None,
            extra = Map("param3" -> 42, "attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")),
            value = "forty two"
          )
        )
      ),
      systemInfo = NameAndVersion("xxx", "777"),
      agentInfo = NameAndVersion("yyy", "777"),
      extraInfo = Map(
        "param3" -> 42,
        "nestedParam" -> Some(Seq(Map("attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42"))))
      )
    )

    val planDTO = v1_2.ExecutionPlan(
      id = Some(UUID.fromString("00000000-0000-0000-0000-000000000000")),
      name = Some("Foo Plan"),
      discriminator = None,
      labels = Some(Map("lbl1" -> Seq("a", "b"))),
      operations = v1_2.Operations(
        write = v1_2.WriteOperation(
          outputSource = "aaa",
          append = true,
          id = "op-0",
          name = Some("Write Operation"),
          childIds = Seq("op-1"),
          params = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          )),
          extra = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          ))
        ),
        reads = Some(Seq(v1_2.ReadOperation(
          inputSources = Seq("bbb"),
          id = "op-2",
          name = Some("Read Operation"),
          output = Some(Seq("attr-1", "attr-2")),
          params = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          )),
          extra = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          ))
        ))),
        other = Some(Seq(v1_2.DataOperation(
          id = "op-1",
          name = Some("Data Operation"),
          childIds = Some(Seq("op-2")),
          output = Some(Seq("attr-3")),
          params = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          )),
          extra = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          ))
        )))
      ),
      attributes = Some(Seq(
        v1_2.Attribute(
          id = "attr-1",
          dataType = Some(dummyDataType),
          childRefs = None,
          extra = Some(Map(
            "param3" -> 42,
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          )),
          name = "A"
        ),
        v1_2.Attribute(
          id = "attr-2",
          dataType = Some(dummyDataType),
          childRefs = None,
          extra = None,
          name = "B"
        ),
        v1_2.Attribute(
          id = "attr-3",
          dataType = Some(dummyDataType),
          childRefs = None,
          extra = None,
          name = "C"
        )
      )),
      expressions = Some(v1_2.Expressions(
        functions = Some(Seq(
          v1_2.FunctionalExpression(
            id = "e1",
            dataType = Some(dummyDataType),
            childRefs = Some(Seq(v1_2.AttrOrExprRef(Some("a1"), None))),
            name = "Expr1",
            params = Some(Map(
              "param3" -> 42,
              "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
              "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
            )),
            extra = Some(Map(
              "param3" -> 42,
              "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
              "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
            ))
          )
        )),
        constants = Some(Seq(
          v1_2.Literal(
            id = "c1",
            dataType = None,
            extra = Some(Map(
              "param3" -> 42,
              "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
              "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
            )),
            value = "forty two"
          )
        ))
      )),
      systemInfo = v1_2.NameAndVersion("xxx", "777"),
      agentInfo = Some(v1_2.NameAndVersion("yyy", "777")),
      extraInfo = Some(Map(
        "param3" -> 42,
        "nestedParam" -> Some(Seq(Map(
          "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
          "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
        )))
      ))
    )

    mapper.toDTO(planEntity) shouldEqual Some(planDTO)
  }

  for (error <- Seq(None, Some("oops"))) yield
    it should s"convert ExecutionEvent entity with error $error to DTO ver 1.2" in {
      val eventEntity = ExecutionEvent(
        planId = UUID.fromString("00000000-0000-0000-0000-000000000000"),
        discriminator = Some("foo"),
        labels = Map("lbl1" -> Seq("a", "b")),
        timestamp = 123456789,
        durationNs = Some(555),
        error = error,
        extra = Map(
          "param3" -> 42,
          "nestedParam" -> Some(Seq(Map("attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42"))))
        )
      )

      val eventDTO = v1_2.ExecutionEvent(
        planId = UUID.fromString("00000000-0000-0000-0000-000000000000"),
        discriminator = Some("foo"),
        labels = Some(Map("lbl1" -> Seq("a", "b"))),
        timestamp = 123456789,
        durationNs = Some(555),
        error = error,
        extra = Some(Map(
          "param3" -> 42,
          "nestedParam" -> Some(Seq(Map(
            "attId" -> v1_2.AttrOrExprRef(Some("attr-42"), None),
            "expId" -> v1_2.AttrOrExprRef(None, Some("expr-42"))
          )))
        ))
      )

      mapper.toDTO(eventEntity) shouldEqual Some(eventDTO)
    }
}
