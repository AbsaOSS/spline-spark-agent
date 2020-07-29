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

package za.co.absa.spline.harvester.builder.read

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.spline.harvester.builder.{PluggableDataSourceFormatResolver, SourceIdentifier}
import za.co.absa.spline.harvester.plugin.Plugin.ReadNodeInfo
import za.co.absa.spline.harvester.plugin.ReadNodeProcessing
import za.co.absa.spline.harvester.plugin.registry.PluginRegistry


class PluggableReadCommandExtractor(pluginRegistry: PluginRegistry)
  extends ReadCommandExtractor {

  private val processFn: LogicalPlan => Option[ReadNodeInfo] =
    pluginRegistry.plugins[ReadNodeProcessing]
      .map(_.readNodeProcessor)
      .reduce(_ orElse _)
      .lift

  // fixme: shouldn't a format name resolving happen outside the command extractor?
  private val dataSourceFormatResolver = new PluggableDataSourceFormatResolver(pluginRegistry)

  override def asReadCommand(operation: LogicalPlan): Option[ReadCommand] = {
    processFn(operation).map({
      case (SourceIdentifier(maybeFormat, uris@_*), params) =>
        val maybeResolvedFormat = maybeFormat.map(dataSourceFormatResolver.resolve)
        val sourceId = SourceIdentifier(maybeResolvedFormat, uris: _*)
        ReadCommand(sourceId, operation, params)
    })
  }

}
