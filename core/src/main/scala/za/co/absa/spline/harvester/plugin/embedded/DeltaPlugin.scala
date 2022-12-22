/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.plugin.embedded

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DeltaPlugin._
import za.co.absa.spline.harvester.plugin.{Plugin, ReadNodeProcessing, WriteNodeProcessing}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import javax.annotation.Priority
import scala.language.reflectiveCalls

@Priority(Precedence.Normal)
class DeltaPlugin(
  pathQualifier: PathQualifier,
  session: SparkSession
) extends Plugin
    with WriteNodeProcessing
    with ReadNodeProcessing {

  override val writeNodeProcessor: PartialFunction[(FuncName, LogicalPlan), WriteNodeInfo] = {
    case (_, `_: DeleteCommand`(command)) =>
      val logicalRelation = extractLogicalRelation(command, "target")
      val condition = extractValue[Option[Expression]](command, "condition")
      val catalogTable = logicalRelation.catalogTable.get
      val uri = catalogTable.location.toString

      val sourceId = SourceIdentifier(Some("delta"), uri)
      val params = Map("condition" -> condition.map(_.toString))
      WriteNodeInfo(sourceId, SaveMode.Overwrite, SyntheticDeltaRead(logicalRelation.output, sourceId, params, logicalRelation), params)

    case (_, `_: UpdateCommand`(command)) =>
      val logicalRelation = extractLogicalRelation(command, "target")
      val condition = extractValue[Option[Expression]](command, "condition")
      val updateExpressions = extractValue[Seq[Expression]](command, "updateExpressions")
      val catalogTable = logicalRelation.catalogTable.get
      val uri = catalogTable.location.toString

      val sourceId = SourceIdentifier(Some("delta"), uri)
      val params = Map("condition" -> condition.map(_.toString), "updateExpressions" -> updateExpressions.map(_.toString()))
      WriteNodeInfo(sourceId, SaveMode.Overwrite, SyntheticDeltaRead(logicalRelation.output, sourceId, params, logicalRelation), params)

    case (_, `_: MergeIntoCommand`(command)) =>
      val targetRelation = extractLogicalRelation(command, "target")
      val targetCatalogTable = targetRelation.catalogTable.get
      val targetUri = targetCatalogTable.location.toString
      val targetId = SourceIdentifier(Some("delta"), targetUri)
      val syntheticTargetRead = SyntheticDeltaRead(targetRelation.output, targetId, Map.empty[String, Any], targetRelation)

      val sourceRelation = extractLogicalRelation(command, "source")
      val sourceCatalogTable = sourceRelation.catalogTable.get
      val sourceUri = sourceCatalogTable.location.toString
      val sourceId = SourceIdentifier(Some("delta"), sourceUri)
      val syntheticSourceRead = SyntheticDeltaRead(sourceRelation.output, sourceId, Map.empty[String, Any], sourceRelation)
      val params = Map.empty[String, Any]

      val condition = extractValue[Expression](command, "condition").toString
      val matchedClauses = extractValue[Seq[Any]](command, "matchedClauses").map(_.toString)
      val notMatchedClauses = extractValue[Seq[Any]](command, "notMatchedClauses").map(_.toString)

      val merge = SyntheticDeltaMerge(targetRelation.output, syntheticSourceRead, syntheticTargetRead, condition, matchedClauses, notMatchedClauses)

      WriteNodeInfo(targetId, SaveMode.Overwrite, merge, params)
  }

  private def extractLogicalRelation(o: AnyRef, fieldName: String): LogicalRelation = {
    val logicalPlan = extractValue[LogicalPlan](o, fieldName)
    extractValue[LogicalRelation](logicalPlan, "child")
  }

  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case SyntheticDeltaRead(output, sourceId, writeParams, logicalPlan) =>
      val readParams = Map.empty[String, Any]
      ReadNodeInfo(sourceId, readParams)
  }
}

object DeltaPlugin {

  case class SyntheticDeltaRead(
    output: Seq[Attribute],
    sourceId: SourceIdentifier,
    writeParams: Map[String, Any],
    logicalPlan: LogicalPlan
  ) extends LeafNode

  case class SyntheticDeltaMerge(
    output: Seq[Attribute],
    left: LogicalPlan,
    right: LogicalPlan,
    condition: String,
    matchedClauses: Seq[String],
    notMatchedClauses: Seq[String]
  ) extends BinaryNode {

    protected def withNewChildrenInternal(newLeft: LogicalPlan, newRight: LogicalPlan): LogicalPlan =
      throw new UnsupportedOperationException()
  }

  private object `_: DeleteCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.delta.commands.DeleteCommand")

  private object `_: UpdateCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.delta.commands.UpdateCommand")

  private object `_: MergeIntoCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.delta.commands.MergeIntoCommand")

}
