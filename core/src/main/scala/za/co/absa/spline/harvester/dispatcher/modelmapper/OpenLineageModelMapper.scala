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
  namespace: String,
  plan: ExecutionPlan,
  event: ExecutionEvent
) {
  private val attrMap = plan.attributes.map(a => a.id -> a).toMap
  private val funcMap = plan.expressions.functions.map(f => f.id -> f).toMap
  private val typeMap = plan.extraInfo("dataTypes").asInstanceOf[Seq[DataType]].map(t => t.id.toString -> t).toMap

  private val writeChild = plan.operations.write.childIds.head
  private val writeOutput = plan.operations.other.find(_.id == writeChild)
    .orElse(plan.operations.reads.find(_.id == writeChild)).get.output


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
      fields = createFieldMap,
      dataset = None,
    )

  private def createFieldMap: Map[String, ColumnLineage] = {
    writeOutput.map { attrId =>
      val attr = attrMap(attrId)

      attr.name -> ColumnLineage(
        inputFields = getLeaves(attr)
          .filter{ case (dep, _) => plan.operations.reads.exists(_.output.contains(dep.id)) }
          .map { case (dep, funChain) =>
          InputField(
            namespace = namespace,
            name = plan.operations.reads
              .find(_.output.contains(dep.id))
              .map(_.inputSources.mkString(", "))
              .getOrElse("N/A"),
            field = dep.name,
            transformations =
              Option(funChain.map{
                f =>
                  InputFieldTransformation(
                    `type` = "DIRECT",
                    subtype = Option(getTransformationSubtype(f)),
                    description = Option(getTransformationName(f))
                  )
              }
          ))
        }.toSeq
      )
    }.toMap
  }

  private def getLeaves(attr: Attribute): Map[Attribute, Seq[FunctionalExpression]] = {
    getLeavesRec(attr.childRefs.map(_.id).toList, Nil, Map.empty)
  }

  @tailrec
  private def getLeavesRec(
    ids: List[String],
    funChain: List[FunctionalExpression],
    deps: Map[Attribute, Seq[FunctionalExpression]]
  ): Map[Attribute, Seq[FunctionalExpression]] =
    ids match {
      case Nil => deps
      case attrId :: tail if attrId.startsWith("attr") =>
        val attr = attrMap(attrId)
        if (attr.childRefs.isEmpty) {
          val newDeps = deps + (attr -> funChain)
          getLeavesRec(tail, funChain, newDeps)
        } else {
          val newIds = tail ++ attr.childRefs.map(_.id)
          getLeavesRec(newIds, funChain, deps)
        }
      case exprId :: tail  if exprId.startsWith("expr") =>
        val funcOption = funcMap.get(exprId)
        val newIds = tail ++ funcOption.map(_.childRefs.map(_.id)).getOrElse(Nil)
        val newFunChain = funcOption.map(f => f :: funChain).getOrElse(funChain)
        getLeavesRec(newIds, newFunChain, deps)
    }

  private def getTransformationSubtype(func: FunctionalExpression): String = {
    func.extra.get(ExprV1.TypeHint).getOrElse("") match {
      case ExprV1.Types.Alias => "IDENTITY"
      case ExprV1.Types.Binary => "TRANSFORMATION"
      case ExprV1.Types.UDF => "TRANSFORMATION"
      case ExprV1.Types.GenericLeaf => "TRANSFORMATION"
      case ExprV1.Types.Generic =>
        if (func.name == "aggregateexpression")
          "AGGREGATION"
        else
          "TRANSFORMATION"
      case ExprV1.Types.UntypedExpression => "TRANSFORMATION"
      case _ => "TRANSFORMATION"
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
  private val SchemaDatasetFacetUrl = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"

  object EventType {
    val Start = "START"
    val Complete = "COMPLETE"
    val Fail = "FAIL"
  }

  private val SplineEvent = "splineEvent"
  private val SplinePlan = "splineEvent"
}
