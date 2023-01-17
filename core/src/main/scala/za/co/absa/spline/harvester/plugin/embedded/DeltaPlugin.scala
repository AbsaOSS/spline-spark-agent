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
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DeltaPlugin._
import za.co.absa.spline.harvester.plugin.{Plugin, WriteNodeProcessing}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import javax.annotation.Priority
import scala.language.reflectiveCalls

@Priority(Precedence.Normal)
class DeltaPlugin(
  pathQualifier: PathQualifier,
  session: SparkSession
) extends Plugin with WriteNodeProcessing {

  override val writeNodeProcessor: PartialFunction[(FuncName, LogicalPlan), WriteNodeInfo] = {
    case (_, `_: DeleteCommand`(command)) =>
      val target = extractValue[LogicalPlan](command, "target")

      val condition = extractValue[Option[Expression]](command, "condition")
      val qualifiedPath = extractPath(command, "tahoeFileIndex")

      val sourceId = SourceIdentifier(Some("delta"), qualifiedPath)
      val params = Map("condition" -> condition.map(_.toString))
      WriteNodeInfo(sourceId, SaveMode.Overwrite, target, params)

    case (_, `_: UpdateCommand`(command)) =>
      val target = extractValue[LogicalPlan](command, "target")

      val condition = extractValue[Option[Expression]](command, "condition")
      val updateExpressions = extractValue[Seq[Expression]](command, "updateExpressions")
      val qualifiedPath = extractPath(command, "tahoeFileIndex")

      val sourceId = SourceIdentifier(Some("delta"), qualifiedPath)
      val params = Map("condition" -> condition.map(_.toString), "updateExpressions" -> updateExpressions.map(_.toString()))
      WriteNodeInfo(sourceId, SaveMode.Overwrite, target, params)

    case (_, `_: MergeIntoCommand`(command)) =>
      val target = extractValue[LogicalPlan](command, "target")
      val source = extractValue[LogicalPlan](command, "source")

      val qualifiedPath = extractPath(command, "targetFileIndex")
      val targetId = SourceIdentifier(Some("delta"), qualifiedPath)

      val condition = extractValue[Expression](command, "condition").toString
      val matchedClauses = extractValue[Seq[Any]](command, "matchedClauses").map(_.toString)
      val notMatchedClauses = extractValue[Seq[Any]](command, "notMatchedClauses").map(_.toString)

      val merge = SyntheticDeltaMerge(target.output, source, target, condition, matchedClauses, notMatchedClauses)

      WriteNodeInfo(targetId, SaveMode.Overwrite, merge, Map.empty[String, Any])
  }

  private def extractPath(command: AnyRef, fieldName: String): String = {
    val targetFileIndex = extractValue[AnyRef](command, fieldName)
    val path = extractValue[org.apache.hadoop.fs.Path](targetFileIndex, "path")
    pathQualifier.qualify(path.toString)
  }

}

object DeltaPlugin {

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
