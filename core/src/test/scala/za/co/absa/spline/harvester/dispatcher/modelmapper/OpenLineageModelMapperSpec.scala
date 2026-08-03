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
import za.co.absa.spline.harvester.ModelConstants.ExecutionPlanExtra
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.column.ColumnLineageDatasetFacet
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.schema.SchemaDatasetFacet
import za.co.absa.spline.harvester.dispatcher.openlineage.model.openlineage.v2_0_2.Job
import za.co.absa.spline.model.dt.Simple
import za.co.absa.spline.producer.model._

import java.util.UUID

class OpenLineageModelMapperSpec
  extends AnyFlatSpec
    with Matchers
    with MockitoSugar {

  behavior of "toDtos()"

  it should "convert ExecutionPlan entity to Open Lineaged Dtos" in {
    val stringDataType = Simple(UUID.randomUUID(), "StringType", false)
    val integerDataType = Simple(UUID.randomUUID(), "IntegerType", false)

    val inputAttr1 = Attribute(
      id = "attr-1",
      dataType = Some(stringDataType.id),
      childRefs = Seq.empty,
      extra = Map.empty,
      name = "inA"
    )

    val inputAttr2 = Attribute(
      id = "attr-2",
      dataType = Some(stringDataType.id),
      childRefs = Seq.empty,
      extra = Map.empty,
      name = "inB"
    )

    val concatExpr = FunctionalExpression(
      id = "expr-1",
      dataType = Some(stringDataType.id),
      childRefs = Seq(AttrRef(inputAttr1.id), AttrRef(inputAttr2.id)),
      extra = Map.empty,
      name = "Concatenation",
      params = Map.empty
    )

    val outputAttr = Attribute(
      id = "attr-3",
      dataType = Some(stringDataType.id),
      childRefs = Seq(ExprRef(concatExpr.id)),
      extra = Map.empty,
      name = "outC"
    )

    val passThroughAttr = Attribute(
      id = "attr-4",
      dataType = Some(integerDataType.id),
      childRefs = Seq.empty,
      extra = Map.empty,
      name = "passThrough"
    )

    val planEntity = ExecutionPlan(
      id = Some(UUID.fromString("00000000-0000-0000-0000-000000000000")),
      name = "Foo Plan",
      discriminator = None,
      labels = Map("lbl1" -> Seq("a", "b")),
      operations = Operations(
        write = WriteOperation(
          outputSource = "file:/data/output/batch/job1_results",
          append = true,
          id = "op-0",
          name = "Write Operation",
          childIds = Seq("op-1"),
          params = Map.empty,
          extra = Map.empty
        ),
        reads = Seq(
          ReadOperation(
            inputSources = Seq("file:/data/input/batch/wikidata.csv"),
            id = "op-2",
            name = "Read Operation",
            output = Seq(inputAttr1.id, passThroughAttr.id),
            params = Map.empty,
            extra = Map.empty
          ),
          ReadOperation(
            inputSources = Seq("file:/data/input/batch/other.csv"),
            id = "op-3",
            name = "Read Operation",
            output = Seq(inputAttr2.id),
            params = Map.empty,
            extra = Map.empty
          )
        ),
        other = Seq(DataOperation(
          id = "op-1",
          name = "Data Operation",
          childIds = Seq("op-2", "op-3"),
          output = Seq(outputAttr.id, passThroughAttr.id),
          params = Map.empty,
          extra = Map.empty
        ))
      ),
      attributes = Seq(inputAttr1, inputAttr2, outputAttr, passThroughAttr),
      expressions = Expressions(
        functions = Seq(concatExpr),
        constants = Seq(
          Literal(
            id = "c1",
            dataType = None,
            extra = Map.empty,
            value = "forty two"
          )
        )
      ),
      systemInfo = NameAndVersion("xxx", "777"),
      agentInfo = NameAndVersion("yyy", "777"),
      extraInfo = Map(
        "param3" -> 42,
        "nestedParam" -> Some(Seq(Map("attId" -> AttrRef("attr-42"), "expId" -> ExprRef("expr-42")))),
        ExecutionPlanExtra.DataTypes -> Seq(stringDataType, integerDataType)
      )
    )

    val event = ExecutionEvent(
      planId = planEntity.id.get,
      labels = Map.empty[String, Seq[String]],
      timestamp = 1785254400000L,
      durationNs = Some(60000000000L), // 1 minute
      discriminator = None,
      error = None,
      extra = Map.empty[String, Any]
    )


    val mapper = new OpenLineageModelMapper("local", planEntity, event)
    val dtos = mapper.toDtos


    dtos.size shouldBe 2

    val startEvent = dtos(0)
    startEvent.eventType.get shouldEqual OpenLineageModelMapper.EventType.Start
    startEvent.eventTime shouldBe "2026-07-28T15:59:00Z"
    startEvent.job shouldBe Job("local", "Foo Plan", None)

    val completeEvent = dtos(1)
    completeEvent.eventType.get shouldEqual OpenLineageModelMapper.EventType.Complete
    completeEvent.eventTime shouldBe "2026-07-28T16:00:00Z"
    completeEvent.job shouldBe Job("local", "Foo Plan", None)

    val input1Dataset = completeEvent.inputs.get.find(_.name == "/data/input/batch/wikidata.csv").get
    input1Dataset.namespace shouldBe "file"

    val in1SchemaFacet = input1Dataset.facets.get("schema").asInstanceOf[SchemaDatasetFacet]
    in1SchemaFacet.fields(0).name shouldEqual "inA"
    in1SchemaFacet.fields(0).`type`.get shouldEqual "StringType"
    in1SchemaFacet.fields(1).name shouldEqual "passThrough"
    in1SchemaFacet.fields(1).`type`.get shouldEqual "IntegerType"

    val input2Dataset = completeEvent.inputs.get.find(_.name == "/data/input/batch/other.csv").get
    input2Dataset.namespace shouldBe "file"

    val in2SchemaFacet = input2Dataset.facets.get("schema").asInstanceOf[SchemaDatasetFacet]
    in2SchemaFacet.fields(0).name shouldEqual "inB"
    in2SchemaFacet.fields(0).`type`.get shouldEqual "StringType"

    val outputDataset = completeEvent.outputs.get(0)
    outputDataset.namespace shouldBe "file"
    outputDataset.name shouldBe "/data/output/batch/job1_results"
    val outSchemaFacet = outputDataset.facets.get("schema").asInstanceOf[SchemaDatasetFacet]
    outSchemaFacet.fields(0).name shouldEqual "outC"
    outSchemaFacet.fields(0).`type`.get shouldEqual "StringType"
    outSchemaFacet.fields(1).name shouldEqual "passThrough"
    outSchemaFacet.fields(1).`type`.get shouldEqual "IntegerType"

    val lineageFacet = outputDataset.facets.get("columnLineage").asInstanceOf[ColumnLineageDatasetFacet]
    lineageFacet.fields("outC") should not be null
    val outCLineage = lineageFacet.fields("outC")
    val inAField = outCLineage.inputFields.find(_.field == "inA").get
    inAField.namespace shouldBe "file"
    inAField.name shouldEqual "/data/input/batch/wikidata.csv"
    val inBField = outCLineage.inputFields.find(_.field == "inB").get
    inBField.namespace shouldBe "file"
    inBField.name shouldEqual "/data/input/batch/other.csv"

    lineageFacet.fields("passThrough") should not be null
    val passThroughLineage = lineageFacet.fields("passThrough")
    val passThroughField = passThroughLineage.inputFields.find(_.field == "passThrough").get
    passThroughField.namespace shouldBe "file"
    passThroughField.name shouldEqual "/data/input/batch/wikidata.csv"

  }
}
