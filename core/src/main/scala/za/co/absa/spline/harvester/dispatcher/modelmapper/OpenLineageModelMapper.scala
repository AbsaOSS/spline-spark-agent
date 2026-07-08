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
import za.co.absa.spline.harvester.dispatcher.ProducerApiVersion.JsonSchemaURLs
import za.co.absa.spline.harvester.dispatcher.modelmapper.OpenLineageModelMapper._
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.schema.{SchemaDatasetFacet, SchemaDatasetFacetField}
import za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.{ColumnLineage, ColumnLineageDatasetFacet, InputField, SplinePayloadFacet}
import za.co.absa.spline.harvester.dispatcher.openlineage.model.openlineage.v2_0_2._
import za.co.absa.spline.model.dt.DataType
import za.co.absa.spline.producer.model._

import java.time.{Duration, Instant}
import java.util.UUID

class OpenLineageModelMapper(
  splineModelMapper: ModelMapper[_, _],
  apiVersion: Version,
  namespace: String,
  plan: ExecutionPlan,
  event: ExecutionEvent
) {
  private val attrMap = plan.attributes.map(a => a.id -> a).toMap
  private val funcMap = plan.expressions.functions.map(f => f.id -> f).toMap
  private val constMap = plan.expressions.constants.map(f => f.id -> f).toMap
  private val typeMap = plan.extraInfo("dataTypes").asInstanceOf[Seq[DataType]].map(t => t.id.toString -> t).toMap


  def toDtos(): Seq[RunEvent] = {
    val runId = UUID.randomUUID()
    val job = Job(namespace = namespace, name = plan.name, facets = None)

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
        .flatMap(ro => ro.inputSources.map(createInputDataset(ro, plan, _)))
        .toNonEmptyOption,
      outputs = Some(Seq(createOutputDataset(plan))),
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

  private def createInputDataset(op: ReadOperation, plan: ExecutionPlan, source: String): InputDataset = {
    val (namespace, name) = OpenLineageUriMapper.uriToNamespaceAndName(source)
    InputDataset(
      namespace = namespace,
      name = name,
      facets = Some(Map("schema" -> createInputSchema(op, plan))),
      inputFacets = None
    )
  }

  private def createOutputDataset(plan: ExecutionPlan): OutputDataset = {
    val (namespace, name) = OpenLineageUriMapper.uriToNamespaceAndName(plan.operations.write.outputSource)
    OutputDataset(
      namespace = namespace,
      name = name,
      facets = Some(Map(
        "schema" -> createOutpuSchema(plan.operations.write, plan),
        "columnLineage" -> createColumnLineageFacet(plan)
      )),
      outputFacets = None
    )
  }

  private def createInputSchema(op: ReadOperation, plan: ExecutionPlan): SchemaDatasetFacet =
    createSchema(op.output, plan)

  private def createOutpuSchema(op: WriteOperation, plan: ExecutionPlan): SchemaDatasetFacet = {
    val childId = op.childIds.head
    val childOp = plan.operations.other.find(_.id == childId)
      .orElse(plan.operations.reads.find(_.id == childId)).get

    createSchema(childOp.output, plan)
  }

  private def createSchema(output: Seq[String], plan: ExecutionPlan): SchemaDatasetFacet =
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
      fields = createFieldMap(plan),
      dataset = None //TODO: Option[Seq[InputField]],
    )

  private def createFieldMap(plan: ExecutionPlan): Map[String, ColumnLineage] = {
    val writeChild = plan.operations.write.childIds.head
    val childOp = plan.operations.other.find(_.id == writeChild)
      .orElse(plan.operations.reads.find(_.id == writeChild)).get

    val map = childOp.output.map { attrId =>
      val attr = attrMap(attrId)

      attr.name -> ColumnLineage(
        inputFields = getDependencies(attr).map { dep =>
          InputField(
            namespace = namespace,
            name = plan.operations.reads.find(_.output.contains(dep.id)).map(_.inputSources.head).getOrElse("unknown"),
            field = dep.name,
            transformations = None
          )
        }
      )
    }.toMap

    map
  }

  private def getDependencies(attr: Attribute): Seq[Attribute] = {
    if (attr.childRefs.isEmpty) {
      Seq(attr)
    } else {
      attr.childRefs.map(_.id).map {
        case attrId: String if attrId.startsWith("attr") =>
          getDependencies(attrMap(attrId))
        case exprId: String if exprId.startsWith("expr") =>
          getDependencies(funcMap(exprId))
      }.flatten
    }
  }

  private def getDependencies(func: FunctionalExpression): Seq[Attribute] = {
    func.childRefs.map(_.id).map {
      case attrId: String if attrId.startsWith("attr") =>
        getDependencies(attrMap(attrId))
      case exprId: String if exprId.startsWith("expr") =>
        funcMap.get(exprId).map(getDependencies).getOrElse(Seq.empty)
    }.flatten
  }


    sealed trait Dependency
  case class DirectDep(subtype: String, masking: Boolean) extends Dependency
  case class IndirectDep(subtype: String, masking: Boolean) extends Dependency


}

object OpenLineageModelMapper {
  private val Producer = s"https://github.com/AbsaOSS/spline-spark-agent/tree/release/${LineageHarvester.SplineVersionInfo.version}"
  private val SchemaUrl = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
  private val PayloadFacetSchemaUrl = "https://cdn.jsdelivr.net/gh/AbsaOSS/spline@api-doc/schemas/openlineage/spline-payload-facet-1.0.json"
  private val columnLineageFacetSchemaUrl = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
  private val SchemaDatasetFacetUrl = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"

  object EventType {
    val Start = "START"
    val Complete = "COMPLETE"
    val Fail = "FAIL"
  }

  private val SplineEvent = "splineEvent"
  private val SplinePlan = "splineEvent"
}
