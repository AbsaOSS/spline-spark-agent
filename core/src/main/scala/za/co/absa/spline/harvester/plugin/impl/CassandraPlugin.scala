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
import org.apache.spark.sql.cassandra.TableRef
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.impl.CassandraPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationPlugin, DataSourceTypePlugin, Plugin}


class CassandraPlugin
  extends Plugin
    with BaseRelationPlugin
    with DataSourceTypePlugin {

  import za.co.absa.commons.ExtractorImplicits._

  override def baseRelProcessor: PartialFunction[(BaseRelation, LogicalRelation), (SourceIdentifier, Params)] = {
    case (`_: CassandraSourceRelation`(casr), _) =>
      val tableRef = extractFieldValue[TableRef](casr, "tableRef")
      val table = tableRef.table
      val keyspace = tableRef.keyspace
      (asSourceId(keyspace, table), Map.empty)
  }

  override def dataSourceTypeProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), (SourceIdentifier, SaveMode, LogicalPlan, Params)] = {
    case (st, cmd) if st == "org.apache.spark.sql.cassandra" || CassandraSourceExtractor.matches(st) =>
      val keyspace = cmd.options("keyspace")
      val table = cmd.options("table")
      (asSourceId(keyspace, table), cmd.mode, cmd.query, cmd.options)
  }
}

object CassandraPlugin {

  object `_: CassandraSourceRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.cassandra.CassandraSourceRelation")

  private object CassandraSourceExtractor extends SafeTypeMatchingExtractor(classOf[org.apache.spark.sql.cassandra.DefaultSource])

  private def asSourceId(keyspace: String, table: String) = SourceIdentifier(Some("cassandra"), s"cassandra:$keyspace:$table")

}
