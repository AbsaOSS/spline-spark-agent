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

import org.apache.spark.sql.catalyst.plans.logical.{Command, LeafNode}
import za.co.absa.spline.harvester.LineageHarvester.{PlanOrRdd, PlanWrap, RddWrap}
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.builder.dsformat.DataSourceFormatResolver
import za.co.absa.spline.harvester.plugin.Plugin.ReadNodeInfo
import za.co.absa.spline.harvester.plugin.registry.PluginRegistry
import za.co.absa.spline.harvester.plugin.{RddReadNodeProcessing, ReadNodeProcessing}

import scala.PartialFunction.condOpt

class PluggableReadCommandExtractor(
  pluginRegistry: PluginRegistry,
  dataSourceFormatResolver: DataSourceFormatResolver
) extends ReadCommandExtractor {

  private val planProcessFn =
    pluginRegistry.plugins[ReadNodeProcessing]
      .map(_.readNodeProcessor)
      .reduce(_ orElse _)

  private val rddProcessFn =
    pluginRegistry.plugins[RddReadNodeProcessing]
      .map(_.rddReadNodeProcessor)
      .reduce(_ orElse _)

  override def asReadCommand(planOrRdd: PlanOrRdd): Option[ReadCommand] = {
    val res = planOrRdd match {
      case PlanWrap(plan) => condOpt(plan) {
        case _: LeafNode | _: Command if planProcessFn.isDefinedAt(plan) =>
          planProcessFn(plan)
      }
      case RddWrap(rdd) => condOpt(rdd) {
        case _ if rddProcessFn.isDefinedAt(rdd) =>
          rddProcessFn(rdd)
      }
    }

    res.map({
      case ReadNodeInfo(SourceIdentifier(maybeFormat, uris @ _*), params, extras) =>
        val maybeResolvedFormat = maybeFormat.map(dataSourceFormatResolver.resolve)
        val sourceId = SourceIdentifier(maybeResolvedFormat, uris: _*)
        ReadCommand(sourceId, params, extras)
    })
  }

}
