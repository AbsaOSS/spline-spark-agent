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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.composite.LogicalRelationPlugin
import za.co.absa.spline.harvester.plugin.impl.HivePlugin
import za.co.absa.spline.harvester.qualifier.PathQualifier


class PluggableReadCommandExtractor(
  pathQualifier: PathQualifier,
  session: SparkSession,
  relationHandler: ReadRelationHandler) extends ReadCommandExtractor {

  private val readPlugins = Seq(
    new HivePlugin(pathQualifier, session),
    new LogicalRelationPlugin(pathQualifier, session)
  )

  // Fixme: obtain from plugins
  private val processFn: LogicalPlan => Option[(SourceIdentifier, Params)] =
    readPlugins
      .map(_.readNodeProcessor)
      .reduce(_ orElse _)
      .orElse[LogicalPlan, (SourceIdentifier, Params)]({
        // Fixme: this should go
        // Other ...
        case lr: LogicalRelation =>
          val br = lr.relation
          if (relationHandler.isApplicable(br)) {
            val ReadCommand(sourceId, _, params) = relationHandler(br, lr)
            (sourceId, params)
          } else {
            sys.error(s"Relation is not supported: $br")
          }
      })
      .lift

  override def asReadCommand(operation: LogicalPlan): Option[ReadCommand] = {
    processFn(operation).map({
      case (sourceId, params) => ReadCommand(sourceId, operation, params)
    })
  }

}
