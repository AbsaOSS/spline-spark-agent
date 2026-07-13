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

package za.co.absa.spline.harvester.dispatcher.modelmapper

import za.co.absa.spline.commons.lang.extensions.NonOptionExtension._
import za.co.absa.spline.commons.lang.extensions.TraversableExtension._
import za.co.absa.spline.commons.version.Version
import za.co.absa.spline.harvester.LineageHarvester
import za.co.absa.spline.harvester.ModelConstants.ExecutionPlanExtra
import za.co.absa.spline.harvester.converter.ExpressionConverter.ExprV1
import za.co.absa.spline.harvester.dispatcher.ProducerApiVersion.JsonSchemaURLs
import za.co.absa.spline.harvester.dispatcher.modelmapper.OpenLineageModelMapper._
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet._
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.column.{ColumnLineage, ColumnLineageDatasetFacet, InputField, InputFieldTransformation}
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.schema.{SchemaDatasetFacet, SchemaDatasetFacetField}
import za.co.absa.spline.harvester.dispatcher.openlineage.model.openlineage.v2_0_2._
import za.co.absa.spline.model.dt.DataType
import za.co.absa.spline.producer.model._

import java.time.{Duration, Instant}
import java.util.UUID
import scala.annotation.tailrec

class OpenLineageModelMapper(
  splineModelMapper: ModelMapper[_, _],
  apiVersion: Version,
  jobNamespace: String,
  plan: ExecutionPlan,
  event: ExecutionEvent
) {
  private val attrMap = plan.attributes.map(a => a.id -> a).toMap
  private val funcMap = plan.expressions.functions.map(f => f.id -> f).toMap
  private val typeMap = plan.extraInfo(ExecutionPlanExtra.DataTypes).asInstanceOf[Seq[DataType]]
    .map(t => t.id.toString -> t).toMap

  private val writeChild = plan.operations.write.childIds.head
  private val writeOutput = plan.operations.other.find(_.id == writeChild)
    .orElse(plan.operations.reads.find(_.id == writeChild)).get.output


  def toDtos: Seq[RunEvent] = {
    val runId = UUID.randomUUID()
    val job = Job(namespace = jobNamespace, name = plan.name, facets = None)

    val completeTime = Instant.ofEpochMilli(event.timestamp)
    val duration = Duration.ofNanos(event.durationNs.getOrElse(0))
    val startTime = completeTime.minus(duration)

    val eventStart = RunEvent(
      eventType = EventType.Start.toOption,
      eventTime = java.util.Date.from(startTime),
      run = Run(runId = runId, facets = None),
      job = job,
      inputs = None,
      outputs = None,
      producer = Producer,
      schemaURL = SchemaUrl
    )

    val eventCompleted = RunEvent(
      eventType = event.error.map(_ => EventType.Fail).orElse(EventType.Complete.toOption),
      eventTime = java.util.Date.from(completeTime),
      run = Run(runId = runId, facets = Some(Map(
        SplinePlan -> createSplinePayloadFacet(splineModelMapper.toDTO(plan), JsonSchemaURLs.planSchemaForAPIVersion(apiVersion)),
        SplineEvent -> createSplinePayloadFacet(splineModelMapper.toDTO(event), JsonSchemaURLs.eventSchemaForAPIVersion(apiVersion))
      ))),
      job = job,
      inputs = plan.operations.reads
        .flatMap(ro => ro.inputSources.map(createInputDataset(ro, _)))
        .toNonEmptyOption,
      outputs = Option(Seq(createOutputDataset)),
      producer = Producer,
      schemaURL = SchemaUrl
    )

    Seq(eventStart, eventCompleted)
  }

  private def createSplinePayloadFacet(payload: AnyRef, payloadSchemaUrl: String) =
    new SplinePayloadFacet(
      _producer = Producer,
      _schemaURL = PayloadFacetSchemaUrl,
      payloadSchemaURL = payloadSchemaUrl,
      payload = payload
    )

  private def createInputDataset(op: ReadOperation, source: String): InputDataset = {
    val (namespace, name) = OpenLineageUriMapper.uriToNamespaceAndName(source)
    InputDataset(
      namespace = namespace,
      name = name,
      facets = Some(Map("schema" -> createSchema(op.output))),
      inputFacets = None
    )
  }

  private def createOutputDataset: OutputDataset = {
    val (namespace, name) = OpenLineageUriMapper.uriToNamespaceAndName(plan.operations.write.outputSource)
    OutputDataset(
      namespace = namespace,
      name = name,
      facets = Some(Map(
        "schema" -> createSchema(writeOutput),
        "columnLineage" -> createColumnLineageFacet(plan)
      )),
      outputFacets = None
    )
  }

  private def createSchema(output: Seq[String]): SchemaDatasetFacet =
    SchemaDatasetFacet(
      _producer = Producer,
      _schemaURL = SchemaDatasetFacetUrl,
      fields = output.map { attrId =>
        val attr = attrMap(attrId)
        createSchemaField(attr.name, attr.dataType.map(t => typeMap(t.toString)))
      }
    )

  private def createSchemaField(name: String, dataType: Option[DataType]): SchemaDatasetFacetField = {
    import za.co.absa.spline.model.dt._
    dataType.map {
      case Simple(_, typeName, _) =>
        SchemaDatasetFacetField(
          name = name,
          `type` = Option(typeName),
          description = None,
          fields = Seq.empty
        )
      case Struct(_, fields, _) =>
        SchemaDatasetFacetField(
          name = name,
          `type` = Some("struct"),
          description = None,
          fields = fields.map(f => createSchemaField(f.name, typeMap.get(f.dataTypeId.toString)))
        )
      case Array(_, elementDataTypeId, _) =>
        SchemaDatasetFacetField(
          name = name,
          `type` = Some("array"),
          description = None,
          fields = Seq(createSchemaField("_element", typeMap.get(elementDataTypeId.toString)))
        )
    }.getOrElse(
      SchemaDatasetFacetField(
        name = name,
        `type` = None,
        description = None,
        fields = Seq.empty
      )
    )
  }

  private def createColumnLineageFacet(plan: ExecutionPlan): ColumnLineageDatasetFacet =
    ColumnLineageDatasetFacet(
      _producer = Producer,
      _schemaURL = columnLineageFacetSchemaUrl,
      fields = createColumnLineageMap,
      dataset = None
    )

  private def createColumnLineageMap: Map[String, ColumnLineage] = {
    writeOutput.map { attrId =>
      val attr = attrMap(attrId)

      attr.name -> ColumnLineage(
        inputFields = getLeaves(attr)
          .flatMap { case (attr, funChain) =>
            plan.operations.reads
              .find(_.output.contains(attr.id))
              .map(_.inputSources)
              .getOrElse(Seq.empty)
              .map(createLineageInputField(attr, funChain, _))
          }.toSeq
      )
    }.toMap
  }

  private def createLineageInputField(
    attr: Attribute,
    funChain: Seq[FunctionalExpression],
    inputSource: String
  ): InputField = {
    val (namespace, name) = OpenLineageUriMapper.uriToNamespaceAndName(inputSource)
    InputField(
      namespace = namespace,
      name = name,
      field = attr.name,
      transformations =
        Option(funChain.map {
          f =>
            InputFieldTransformation(
              `type` = "DIRECT",
              subtype = Option(getTransformationSubtype(f)),
              description = Option(getTransformationName(f))
            )
        })
    )
  }

  private def getLeaves(attr: Attribute): Map[Attribute, Seq[FunctionalExpression]] = {
    getLeavesRec(attr.childRefs.map(ref =>(ref.id, Nil)).toList, Map.empty)
  }

  @tailrec
  private def getLeavesRec(
    ids: List[(String, List[FunctionalExpression])],
    deps: Map[Attribute, Seq[FunctionalExpression]]
  ): Map[Attribute, Seq[FunctionalExpression]] = ids match {
    case Nil => deps
    case (attrId, funChain) :: tail if attrId.startsWith("attr") =>
      val attr = attrMap(attrId)
      if (attr.childRefs.isEmpty) {
        val newDeps = deps + (attr -> funChain)
        getLeavesRec(tail, newDeps)
      } else {
        val newIds = tail ++ attr.childRefs.map(ref => (ref.id, funChain))
        getLeavesRec(newIds, deps)
      }
    case (exprId, funChain) :: tail  if exprId.startsWith("expr") =>
      val funcOption = funcMap.get(exprId) // when expr is constant we ignore it
      val newFunChain = funcOption.map(f => f :: funChain).getOrElse(funChain)
      val newIds = tail ++ funcOption.map(_.childRefs.map(ref => (ref.id, newFunChain))).getOrElse(Nil)
      getLeavesRec(newIds, deps)
  }

  private def getTransformationSubtype(func: FunctionalExpression): String = {
    func.extra.getOrElse(ExprV1.TypeHint, "") match {
      case ExprV1.Types.Alias => DirectTransformationSubtype.Identity
      case ExprV1.Types.Binary => DirectTransformationSubtype.Transformation
      case ExprV1.Types.UDF =>  DirectTransformationSubtype.Transformation
      case ExprV1.Types.GenericLeaf =>  DirectTransformationSubtype.Transformation
      case ExprV1.Types.Generic =>
        if (func.name == "aggregateexpression")
          DirectTransformationSubtype.Aggregation
        else
          DirectTransformationSubtype.Transformation
      case ExprV1.Types.UntypedExpression =>  DirectTransformationSubtype.Transformation
      case _ =>  DirectTransformationSubtype.Transformation
    }
  }

  private def getTransformationName(func: FunctionalExpression): String = {
    func.extra.get(ExprV1.TypeHint).map(_ + ": ").getOrElse("").concat(func.name)
  }

}

object OpenLineageModelMapper {
  private val Producer = s"https://github.com/AbsaOSS/spline-spark-agent/tree/release/${LineageHarvester.SplineVersionInfo.version}"
  private val SchemaUrl = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
  private val PayloadFacetSchemaUrl = "https://cdn.jsdelivr.net/gh/AbsaOSS/spline@api-doc/schemas/openlineage/spline-payload-facet-1.0.json"
  private val columnLineageFacetSchemaUrl = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
  private val SchemaDatasetFacetUrl = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"

  object EventType {
    val Start = "START"
    val Complete = "COMPLETE"
    val Fail = "FAIL"
  }

  object DirectTransformationSubtype {
    val Identity = "IDENTITY"
    val Transformation = "TRANSFORMATION"
    val Aggregation = "AGGREGATION"
  }


  private val SplineEvent = "splineEvent"
  private val SplinePlan = "splineEvent"
}
