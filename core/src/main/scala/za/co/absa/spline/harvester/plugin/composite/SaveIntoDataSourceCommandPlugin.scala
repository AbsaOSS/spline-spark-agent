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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import za.co.absa.commons.reflect.extractors.AccessorMethodValueExtractor
import za.co.absa.spline.harvester.builder.{DataSourceFormatResolver, SourceIdentifier}
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.composite.SaveIntoDataSourceCommandPlugin._
import za.co.absa.spline.harvester.plugin.impl._
import za.co.absa.spline.harvester.plugin.{Plugin, WritePlugin}
import za.co.absa.spline.harvester.qualifier.PathQualifier

class SaveIntoDataSourceCommandPlugin(pathQualifier: PathQualifier) extends Plugin with WritePlugin {

  // fixme: obtain from a plugin registry
  private val plugins = Seq(
    new JDBCPlugin,
    new CassandraPlugin,
    new MongoPlugin,
    new ElasticSearchPlugin,
    new KafkaPlugin
  )

  private val dstProcessor =
    plugins
      .map(_.dataSourceTypeProcessor)
      .reduce(_ orElse _)


  override def writeNodeProcessor: PartialFunction[LogicalPlan, (SourceIdentifier, SaveMode, LogicalPlan, Params)] = {
    // fixme: should the default case be handled here?
    case cmd: SaveIntoDataSourceCommand =>
      cmd match {
        case DataSourceTypeExtractor(dst)
          if dstProcessor.isDefinedAt((dst, cmd)) =>
          dstProcessor((dst, cmd))

        case _ =>
          val maybeFormat = DataSourceTypeExtractor.unapply(cmd).map(DataSourceFormatResolver.resolve)
          val opts = cmd.options
          val uri = opts.get("path").map(pathQualifier.qualify)
            .getOrElse(sys.error(s"Cannot extract source URI from the options: ${opts.keySet mkString ","}"))
          (SourceIdentifier(maybeFormat, uri), cmd.mode, cmd.query, opts)
      }
  }
}

object SaveIntoDataSourceCommandPlugin {

  private object DataSourceTypeExtractor extends AccessorMethodValueExtractor[AnyRef]("provider", "dataSource")

}
