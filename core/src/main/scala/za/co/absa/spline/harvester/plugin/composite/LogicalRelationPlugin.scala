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

package za.co.absa.spline.harvester.plugin.composite

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import za.co.absa.spline.harvester.plugin.Plugin.ReadNodeInfo
import za.co.absa.spline.harvester.plugin._


class LogicalRelationPlugin(pluginRegistry: PluginRegistry) extends Plugin with ReadPlugin {

  private lazy val baseRelProcessor =
    pluginRegistry.plugins
      .collect({ case p: BaseRelationPlugin => p })
      .map(_.baseRelProcessor)
      .reduce(_ orElse _)

  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case lr: LogicalRelation
      if baseRelProcessor.isDefinedAt((lr.relation, lr)) =>
      baseRelProcessor((lr.relation, lr))
  }
}
