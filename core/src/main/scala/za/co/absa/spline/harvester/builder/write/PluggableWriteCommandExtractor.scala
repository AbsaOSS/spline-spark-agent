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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import za.co.absa.spline.harvester.builder.write.PluggableWriteCommandExtractor._
import za.co.absa.spline.harvester.builder.{PluggableDataSourceFormatResolver, SourceIdentifier}
import za.co.absa.spline.harvester.exception.UnsupportedSparkCommandException
import za.co.absa.spline.harvester.plugin.Plugin.WriteNodeInfo
import za.co.absa.spline.harvester.plugin.{PluginRegistry, WritePlugin}

import scala.language.reflectiveCalls

class PluggableWriteCommandExtractor(pluginRegistry: PluginRegistry)
  extends WriteCommandExtractor {

  private val processFn: LogicalPlan => Option[WriteNodeInfo] =
    pluginRegistry.plugins
      .collect({ case p: WritePlugin => p })
      .map(_.writeNodeProcessor)
      .reduce(_ orElse _)
      .lift

  private val dataSourceFormatResolver = new PluggableDataSourceFormatResolver(pluginRegistry)

  @throws(classOf[UnsupportedSparkCommandException])
  def asWriteCommand(operation: LogicalPlan): Option[WriteCommand] = {
    val maybeCapturedResult = processFn(operation)

    if (maybeCapturedResult.isEmpty) alertWhenUnimplementedCommand(operation)

    maybeCapturedResult.map({
      case (SourceIdentifier(maybeFormat, uris@_*), mode, plan, params) =>
        val maybeResolvedFormat = maybeFormat.map(dataSourceFormatResolver.resolve)
        val sourceId = SourceIdentifier(maybeResolvedFormat, uris: _*)
        WriteCommand(operation.nodeName, sourceId, mode, plan, params)
    })
  }

}

object PluggableWriteCommandExtractor {

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

  private def alertWhenUnimplementedCommand(c: LogicalPlan): Unit = {
    if (commandsToBeImplemented.contains(c.getClass)) throw new UnsupportedSparkCommandException(c)
  }

}
