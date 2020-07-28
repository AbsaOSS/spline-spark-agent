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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcRelationProvider}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.{SourceId, SourceIdentifier}
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.impl.JDBCPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationPlugin, DataSourceTypePlugin, Plugin}


class JDBCPlugin
  extends Plugin
    with BaseRelationPlugin
    with DataSourceTypePlugin {

  override def baseRelProcessor: PartialFunction[(BaseRelation, LogicalRelation), (SourceIdentifier, Params)] = {
    case (`_: JDBCRelation`(jr), _) =>
      val jdbcOptions = extractFieldValue[JDBCOptions](jr, "jdbcOptions")
      val url = extractFieldValue[String](jdbcOptions, "url")
      val params = extractFieldValue[Map[String, String]](jdbcOptions, "parameters")
      val TableOrQueryFromJDBCOptionsExtractor(toq) = jdbcOptions
      (SourceId.forJDBC(url, toq), params)
  }

  override def dataSourceTypeProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), (SourceIdentifier, SaveMode, LogicalPlan, Params)] = {
    case (st, cmd) if st == "jdbc" || st.isInstanceOf[JdbcRelationProvider] =>
      val jdbcConnectionString = cmd.options("url")
      val tableName = cmd.options("dbtable")
      (SourceId.forJDBC(jdbcConnectionString, tableName), cmd.mode, cmd.query, Map.empty)
  }
}

object JDBCPlugin {

  object `_: JDBCRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation")

  object TableOrQueryFromJDBCOptionsExtractor extends AccessorMethodValueExtractor[String]("table", "tableOrQuery")

}



