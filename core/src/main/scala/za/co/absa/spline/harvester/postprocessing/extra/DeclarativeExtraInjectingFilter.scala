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

package za.co.absa.spline.harvester.postprocessing.extra

import org.apache.commons.configuration.Configuration
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import za.co.absa.commons.config.ConfigurationImplicits.ConfigurationOptionalWrapper
import za.co.absa.spline.harvester.ExtraMetadataImplicits._
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.harvester.postprocessing.PostProcessingFilter
import za.co.absa.spline.harvester.postprocessing.extra.DeclarativeExtraInjectingFilter.ExtraDef
import za.co.absa.spline.harvester.postprocessing.extra.model.predicate.{BaseNodeNames, Predicate}
import za.co.absa.spline.harvester.postprocessing.extra.model.template.ExtraTemplate
import za.co.absa.spline.producer.model.v1_1._

import java.net.URL

class DeclarativeExtraInjectingFilter(
  planExtraDefs: Seq[ExtraDef],
  eventExtraDefs: Seq[ExtraDef],
  readExtraDefs: Seq[ExtraDef],
  writeExtraDefs: Seq[ExtraDef],
  operationExtraDefs: Seq[ExtraDef]
) extends PostProcessingFilter {

  override def processExecutionEvent(event: ExecutionEvent, ctx: HarvestingContext): ExecutionEvent = {
    if (eventExtraDefs.isEmpty)
      event
    else
      event.withAddedExtra(evaluateExtraDefs(eventExtraDefs, ctx, BaseNodeNames.ExecutionEvent, event))
  }

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = {
    if (planExtraDefs.isEmpty)
      plan
    else
      plan.withAddedExtra(evaluateExtraDefs(planExtraDefs, ctx, BaseNodeNames.ExecutionPlan, plan))
  }

  override def processReadOperation(read: ReadOperation, ctx: HarvestingContext): ReadOperation = {
    if (readExtraDefs.isEmpty)
      read
    else
      read.withAddedExtra(evaluateExtraDefs(readExtraDefs, ctx, BaseNodeNames.Read, read))
  }

  override def processWriteOperation(write: WriteOperation, ctx: HarvestingContext): WriteOperation = {
    if (writeExtraDefs.isEmpty)
      write
    else
      write.withAddedExtra(evaluateExtraDefs(writeExtraDefs, ctx, BaseNodeNames.Write, write))
  }

  override def processDataOperation(operation: DataOperation, ctx: HarvestingContext): DataOperation = {
    if (operationExtraDefs.isEmpty)
      operation
    else
      operation.withAddedExtra(evaluateExtraDefs(operationExtraDefs, ctx, BaseNodeNames.Operation, operation))
  }

  private def evaluateExtraDefs(defs: Seq[ExtraDef], ctx: HarvestingContext, nodeName: String, node: Any) = {
    val bindings = contextBindings(ctx)
    val jsBindings = bindings + (nodeName -> node)
    val predicateBindings = bindings + ("@" -> node)

    defs
      .filter(_.predicate.eval(predicateBindings))
      .map(_.template.eval(jsBindings))
      .reduceLeftOption(deepMergeMaps)
      .getOrElse(Map.empty)
  }

  private def contextBindings(ctx: HarvestingContext) = Map(
    "logicalPlan" -> ctx.logicalPlan,
    "executedPlanOpt" -> ctx.executedPlanOpt,
    "session" -> ctx.session
  )

  private def deepMergeMaps(m1: Map[String, Any], m2: Map[String, Any]): Map[String, Any] =
    (m1.keySet ++ m2.keySet)
      .map(k => k -> mergeOptionalValues(m1.get(k), m2.get(k)))
      .toMap

  private def mergeOptionalValues(mv1: Option[Any], mv2: Option[Any]): Any = (mv1, mv2) match {
    case (Some(v1), Some(v2)) => mergeValues(v1, v2)
    case (None, Some(v2)) => v2
    case (Some(v1), None) => v1
  }

  private def mergeValues(v1: Any, v2: Any): Any = (v1, v2) match {
    case (v1: Map[String, _], v2: Map[String, _]) => deepMergeMaps(v1, v2)
    case (v1: Seq[Any], v2: Seq[Any]) => (v1 ++ v2).distinct
    case (v1, v2) => v2
  }

}

object DeclarativeExtraInjectingFilter extends Logging {

  val InjectRulesKey = "spline.userExtraMetadata.injectRules"
  val UrlKey = "url"

  case class ExtraDef(nodeName: String, predicate: Predicate, template: ExtraTemplate)

  def apply(conf: Configuration): Option[DeclarativeExtraInjectingFilter] = {
    val maybeExtraDefMap = conf
      .getOptionalString(InjectRulesKey)
      .orElse(conf.getOptionalString(s"$InjectRulesKey.$UrlKey").map(loadFromUrl))
      .map(_.fromJson[Map[String, Map[String, Any]]])

    maybeExtraDefMap
      .map(createFilter)
  }

  private def loadFromUrl(urlString: String): String = {
    IOUtils.toString(new URL(urlString))
  }

  private def createFilter(unparsedExtraDefMap: Map[String, Map[String, Any]]): DeclarativeExtraInjectingFilter = {
    val extraDefMap = unparsedExtraDefMap.toSeq
      .map {
        case (baseKey, extra) =>
          val (name, predicate) = ExtraPredicateParser.parse(baseKey)
          val template = ExtraTemplateParser.parse(extra)
          ExtraDef(name, predicate, template)
      }
      .groupBy(_.nodeName)

    new DeclarativeExtraInjectingFilter(
      extraDefMap.getOrElse(BaseNodeNames.ExecutionPlan, Seq.empty),
      extraDefMap.getOrElse(BaseNodeNames.ExecutionEvent, Seq.empty),
      extraDefMap.getOrElse(BaseNodeNames.Read, Seq.empty),
      extraDefMap.getOrElse(BaseNodeNames.Write, Seq.empty),
      extraDefMap.getOrElse(BaseNodeNames.Operation, Seq.empty)
    )
  }
}
