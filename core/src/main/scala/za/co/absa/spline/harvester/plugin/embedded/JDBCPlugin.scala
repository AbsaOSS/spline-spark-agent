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

package za.co.absa.spline.harvester.plugin.embedded

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcRelationProvider}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.JDBCPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin, RddReadNodeProcessing, RelationProviderProcessing}

import javax.annotation.Priority


@Priority(Precedence.Normal)
class JDBCPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing
    with RddReadNodeProcessing {

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: JDBCRelation`(jr), _) =>
      val jdbcOptions = extractValue[JDBCOptions](jr, "jdbcOptions")
      jdbcOptionsToReadNodeInfo(jdbcOptions)
  }

  override def rddReadNodeProcessor: PartialFunction[RDD[_], ReadNodeInfo] = {
    case `_: JDBCRDD`(jdbcRdd) =>
      val jdbcOptions = extractValue[JDBCOptions](jdbcRdd, "options")
      jdbcOptionsToReadNodeInfo(jdbcOptions)
  }

  private def jdbcOptionsToReadNodeInfo(jdbcOptions: JDBCOptions): ReadNodeInfo = {
    val url = extractValue[String](jdbcOptions, "url")
    val params = extractValue[Map[String, String]](jdbcOptions, "parameters")
    val TableOrQueryFromJDBCOptionsExtractor(toq) = jdbcOptions
    ReadNodeInfo(asSourceId(url, toq), params)
  }

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if rp == "jdbc" || rp.isInstanceOf[JdbcRelationProvider] =>
      val jdbcConnectionString = cmd.options("url")
      val tableName = cmd.options("dbtable")
      WriteNodeInfo(asSourceId(jdbcConnectionString, tableName), cmd.mode, cmd.query, Map.empty)
  }
}

object JDBCPlugin {

  object `_: JDBCRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation")

  object `_: JDBCRDD` extends SafeTypeMatchingExtractor[RDD[InternalRow]]("org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD")

  object TableOrQueryFromJDBCOptionsExtractor extends AccessorMethodValueExtractor[String]("table", "tableOrQuery")

  private def asSourceId(connectionUrl: String, table: String) = SourceIdentifier(Some("jdbc"), s"$connectionUrl:$table")
}
