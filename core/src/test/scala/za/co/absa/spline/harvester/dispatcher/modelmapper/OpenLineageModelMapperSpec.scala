package za.co.absa.spline.harvester.dispatcher.modelmapper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.commons.lang.extensions.NonOptionExtension._
import za.co.absa.spline.commons.lang.extensions.TraversableExtension._
import za.co.absa.spline.harvester.ModelConstants.ExecutionPlanExtra
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
          outputSource = "aaa",
          append = true,
          id = "op-0",
          name = "Write Operation",
          childIds = Seq("op-1"),
          params = Map.empty,
          extra = Map.empty
        ),
        reads = Seq(ReadOperation(
          inputSources = Seq("bbb"),
          id = "op-2",
          name = "Read Operation",
          output = Seq(inputAttr1.id, inputAttr2.id, passThroughAttr.id),
          params = Map.empty,
          extra = Map.empty
        )),
        other = Seq(DataOperation(
          id = "op-1",
          name = "Data Operation",
          childIds = Seq("op-2"),
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
    startEvent.eventTime shouldBe "2026-07-28T15:59Z"
    startEvent.job shouldBe Job("local", "Foo Plan", None)

    val completeEvent = dtos(1)
    completeEvent.eventType.get shouldEqual OpenLineageModelMapper.EventType.Complete
    completeEvent.eventTime shouldBe "2026-07-28T16:00Z"
    completeEvent.job shouldBe Job("local", "Foo Plan", None)



  }
}
