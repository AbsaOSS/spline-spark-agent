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

package za.co.absa.spline.harvester.plugin.impl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.harvester.CatalogTableUtils
import za.co.absa.spline.harvester.builder.{DataSourceFormatNameResolver, SourceIdentifier}
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.{BaseRelationPlugin, Plugin}
import za.co.absa.spline.harvester.qualifier.PathQualifier


class HadoopPlugin(pathQualifier: PathQualifier, session: SparkSession) extends Plugin with BaseRelationPlugin {

  override def baseRelProcessor: PartialFunction[(BaseRelation, LogicalRelation), (SourceIdentifier, Params)] = {

    case (hr: HadoopFsRelation, lr) =>
      lr.catalogTable
        .map(ct => {
          val sourceId = SourceIdentifier.forTable(ct)(pathQualifier, session)
          val params = CatalogTableUtils.extractCatalogTableParams(ct)
          (sourceId, params)
        })
        .getOrElse({
          val uris = hr.location.rootPaths.map(path => pathQualifier.qualify(path.toString))
          val fileFormat = hr.fileFormat
          val formatName = DataSourceFormatNameResolver.resolve(fileFormat)
          (SourceIdentifier(Some(formatName), uris: _*), hr.options)
        })
  }
}



