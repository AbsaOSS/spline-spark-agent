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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DeltaPlugin._
import za.co.absa.spline.harvester.plugin.{Plugin, WriteNodeProcessing}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import javax.annotation.Priority
import scala.language.reflectiveCalls
import scala.util.Try

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

    case (_, `_: MergeIntoCommand`(command)) => extractMerge(command)
    case (_, `_: MergeIntoCommandEdge`(command)) => extractMerge(command)
  }

  private def extractMerge(command: LogicalPlan): WriteNodeInfo = {
    val qualifiedPath = extractPath(command, "targetFileIndex")
    val targetId = SourceIdentifier(Some("delta"), qualifiedPath)

    WriteNodeInfo(targetId, SaveMode.Overwrite, command, Map.empty[String, Any])
  }


  private def extractPath(command: AnyRef, fieldName: String): String = {
    val path = Try {
      val targetFileIndex = extractValue[AnyRef](command, fieldName)
      extractValue[org.apache.hadoop.fs.Path](targetFileIndex, "path")
    } getOrElse {
      val deltaLog = extractValue[AnyRef](command, "deltaLog")
      extractValue[AnyRef](deltaLog, "dataPath")
    }
    pathQualifier.qualify(path.toString)
  }

}

object DeltaPlugin {

  private object `_: DeleteCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.delta.commands.DeleteCommand")

  private object `_: UpdateCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.delta.commands.UpdateCommand")

  object `_: MergeIntoCommand` extends SafeTypeMatchingExtractor[LogicalPlan](
    "org.apache.spark.sql.delta.commands.MergeIntoCommand")

  object `_: MergeIntoCommandEdge` extends SafeTypeMatchingExtractor[LogicalPlan](
    "com.databricks.sql.transaction.tahoe.commands.MergeIntoCommandEdge")

}
