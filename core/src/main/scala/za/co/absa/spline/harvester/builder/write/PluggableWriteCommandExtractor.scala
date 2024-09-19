/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.builder.write

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.builder.dsformat.DataSourceFormatResolver
import za.co.absa.spline.harvester.builder.write.PluggableWriteCommandExtractor.warnIfUnimplementedCommand
import za.co.absa.spline.harvester.plugin.Plugin.WriteNodeInfo
import za.co.absa.spline.harvester.plugin.WriteNodeProcessing
import za.co.absa.spline.harvester.plugin.registry.PluginRegistry

import scala.language.reflectiveCalls

class PluggableWriteCommandExtractor(
  pluginRegistry: PluginRegistry,
  dataSourceFormatResolver: DataSourceFormatResolver
) extends WriteCommandExtractor {

  private val processFn: ((FuncName, LogicalPlan)) => Option[WriteNodeInfo] =
    pluginRegistry.plugins[WriteNodeProcessing]
      .map(_.writeNodeProcessor)
      .reduceOption(_ orElse _)
      .getOrElse(PartialFunction.empty)
      .lift

  def asWriteCommand(funcName: FuncName, logicalPlan: LogicalPlan): Option[WriteCommand] = {
    val maybeCapturedResult = processFn(funcName, logicalPlan)

    if (maybeCapturedResult.isEmpty) warnIfUnimplementedCommand(logicalPlan)

    maybeCapturedResult.map({
      case WriteNodeInfo(SourceIdentifier(maybeFormat, uris @ _*), mode, plan, params, extras, name) =>
        val maybeResolvedFormat = maybeFormat.map(dataSourceFormatResolver.resolve)
        val sourceId = SourceIdentifier(maybeResolvedFormat, uris: _*)
        val cmdName = StringUtils.defaultIfBlank(name, logicalPlan.nodeName)
        WriteCommand(cmdName, sourceId, mode, plan, params, extras)
    })
  }

}

object PluggableWriteCommandExtractor extends Logging {

  private val commandsToBeImplemented = Seq(
    classOf[AlterTableAddColumnsCommand],
    classOf[AlterTableChangeColumnCommand],
    classOf[AlterTableRenameCommand],
    classOf[AlterTableSetLocationCommand],
    classOf[CreateDataSourceTableCommand],
    classOf[CreateDatabaseCommand],
    classOf[CreateTableLikeCommand],
    classOf[DropDatabaseCommand],
    classOf[LoadDataCommand],
    classOf[TruncateTableCommand]
  )

  private def warnIfUnimplementedCommand(c: LogicalPlan): Unit = {
    if (commandsToBeImplemented.contains(c.getClass)) {
      logWarning(s"Spark command was intercepted, but is not yet implemented! Command:'${c.getClass}'")
    }
  }

}
