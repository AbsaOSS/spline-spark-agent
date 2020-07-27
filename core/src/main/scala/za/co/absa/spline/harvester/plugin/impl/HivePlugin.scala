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
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.spline.harvester.CatalogTableUtils
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.{Plugin, ReadPlugin}
import za.co.absa.spline.harvester.qualifier.PathQualifier


class HivePlugin(pathQualifier: PathQualifier, session: SparkSession) extends Plugin with ReadPlugin {

  override val readNodeProcessor: PartialFunction[LogicalPlan, (SourceIdentifier, Params)] = {
    case htr: HiveTableRelation =>
      val catalogTable = htr.tableMeta
      val sourceId = SourceIdentifier.forTable(catalogTable)(pathQualifier, session)
      val params = CatalogTableUtils.extractCatalogTableParams(catalogTable)
      (sourceId, params)
  }
}
