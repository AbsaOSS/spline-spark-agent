/*
 * Copyright 2021 ABSA Group Limited
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

import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.SnowflakePlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin, RelationProviderProcessing}

import javax.annotation.Priority
import scala.language.reflectiveCalls

@Priority(Precedence.Normal)
class SnowflakePlugin(spark: SparkSession)
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing {

  import za.co.absa.spline.commons.ExtractorImplicits._

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: SnowflakeRelation`(r), _) =>
      val params = extractValue[net.snowflake.spark.snowflake.Parameters.MergedParameters](r, "params")

      val url: String = params.sfURL
      val warehouse: String = params.sfWarehouse.getOrElse("")
      val database: String = params.sfDatabase
      val schema: String = params.sfSchema
      val table: String = params.table.getOrElse("").toString

      ReadNodeInfo(asSourceId(url, warehouse, database, schema, table), Map.empty)
  }

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if rp == "net.snowflake.spark.snowflake.DefaultSource" || SnowflakeSourceExtractor.matches(rp) =>
      val url: String = cmd.options("sfUrl")
      val warehouse: String = cmd.options("sfWarehouse")
      val database: String = cmd.options("sfDatabase")
      val schema: String = cmd.options("sfSchema")
      val table: String = cmd.options("dbtable")

      WriteNodeInfo(asSourceId(url, warehouse, database, schema, table), cmd.mode, cmd.query, cmd.options)  }
}

object SnowflakePlugin {

  private object `_: SnowflakeRelation` extends SafeTypeMatchingExtractor[AnyRef]("net.snowflake.spark.snowflake.SnowflakeRelation")

  private object SnowflakeSourceExtractor extends SafeTypeMatchingExtractor(classOf[net.snowflake.spark.snowflake.DefaultSource])

  private def asSourceId(url: String, warehouse: String, database: String, schema: String, table: String) =
    SourceIdentifier(Some("snowflake"), s"snowflake://$url.$warehouse.$database.$schema.$table")
}
