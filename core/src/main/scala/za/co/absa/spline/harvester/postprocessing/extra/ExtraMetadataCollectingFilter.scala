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
import za.co.absa.commons.config.ConfigurationImplicits.ConfigurationRequiredWrapper
import za.co.absa.spline.harvester.ExtraMetadataImplicits._
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.harvester.postprocessing.PostProcessingFilter
import za.co.absa.spline.harvester.postprocessing.extra.ExtraMetadataCollectingFilter.{ExtraDef, createDefs, evaluateExtraDefs}
import za.co.absa.spline.harvester.postprocessing.extra.model.predicate.{BaseNodeName, Predicate}
import za.co.absa.spline.harvester.postprocessing.extra.model.template.ExtraTemplate
import za.co.absa.spline.producer.model.v1_1._

import java.net.URL
import scala.util.Try

class ExtraMetadataCollectingFilter(allDefs: Map[BaseNodeName.Value, Seq[ExtraDef]]) extends PostProcessingFilter {

  def this(conf: Configuration) = this(createDefs(conf))

  override def name = "Extra metadata"

  override def processExecutionEvent(event: ExecutionEvent, ctx: HarvestingContext): ExecutionEvent = {
    withEvaluatedExtra(BaseNodeName.ExecutionEvent, event, ctx)
  }

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = {
    withEvaluatedExtra(BaseNodeName.ExecutionPlan, plan, ctx)
  }

  override def processReadOperation(read: ReadOperation, ctx: HarvestingContext): ReadOperation = {
    withEvaluatedExtra(BaseNodeName.Read, read, ctx)
  }

  override def processWriteOperation(write: WriteOperation, ctx: HarvestingContext): WriteOperation = {
    withEvaluatedExtra(BaseNodeName.Write, write, ctx)
  }

  override def processDataOperation(operation: DataOperation, ctx: HarvestingContext): DataOperation = {
    withEvaluatedExtra(BaseNodeName.Operation, operation, ctx)
  }

  private def withEvaluatedExtra[A: ExtraAdder](name: BaseNodeName.Value, entity: A, ctx: HarvestingContext): A = {
    allDefs
      .get(name)
      .map(defs => {
        val addedExtra = evaluateExtraDefs(name, entity, defs, ctx)
        entity.withAddedExtra(addedExtra)
      })
      .getOrElse(entity)
  }
}

object ExtraMetadataCollectingFilter extends Logging {

  val InjectRulesKey = "rules"

  case class ExtraDef(nodeName: BaseNodeName.Value, predicate: Predicate, template: ExtraTemplate)

  private def createDefs(conf: Configuration): Map[BaseNodeName.Value, Seq[ExtraDef]] = {
    val rulesJsonOrUrl: String = conf.getRequiredString(InjectRulesKey)

    val rulesJson =
      Try(new URL(rulesJsonOrUrl))
        .toOption
        .map(IOUtils.toString) // load from URL, or
        .getOrElse(rulesJsonOrUrl) // treat it as JSON

    val extraDefMap = rulesJson
      .fromJson[Map[String, Map[String, Any]]]
      .toSeq
      .map {
        case (baseKey, extra) =>
          val (name, predicate) = ExtraPredicateParser.parse(baseKey)
          val template = ExtraTemplateParser.parse(extra)
          ExtraDef(name, predicate, template)
      }
      .groupBy(_.nodeName)

    extraDefMap
  }

  private def evaluateExtraDefs(nodeName: BaseNodeName.Value, node: Any, defs: Seq[ExtraDef], ctx: HarvestingContext): Map[String, Any] = {
    if (defs.isEmpty) {
      Map.empty
    }
    else {
      val bindings = contextBindings(ctx)
      val jsBindings = bindings + (nodeName -> node)
      val predicateBindings = bindings + ("@" -> node)

      defs
        .filter(_.predicate.eval(predicateBindings))
        .map(_.template.eval(jsBindings))
        .reduceLeftOption(deepMergeMaps)
        .getOrElse(Map.empty)
    }
  }

  private def contextBindings(ctx: HarvestingContext): Map[String, Any] = Map(
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
    case (_, v2) => v2
  }
}
