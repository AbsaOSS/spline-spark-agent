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

import com.google.cloud.bigquery.TableId
import javax.annotation.Priority
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.BigQueryPlugin.{`_: BigQuerySourceRelation`, _}
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin, RelationProviderProcessing}


@Priority(Precedence.Normal)
class BigQueryPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing {

  import za.co.absa.commons.ExtractorImplicits._

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: BigQuerySourceRelation`(bq), _) =>

      val tableId = extractFieldValue[TableId](bq, "tableId")
      val project = tableId.getProject
      val schema = tableId.getDataset
      val table = tableId.getTable

      (asSourceId(project, schema, table), Map.empty)

  }

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if rp == "com.google.cloud.spark.bigquery" || BigQuerySourceExtractor.matches(rp) =>
      val schema = cmd.options("schema")
      val table = cmd.options("table")
      val project = cmd.options("project")
      (asSourceId(project, schema, table), cmd.mode, cmd.query, cmd.options)
  }
}

object BigQueryPlugin {

  object `_: BigQuerySourceRelation` extends SafeTypeMatchingExtractor[AnyRef]("com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation")

  private object BigQuerySourceExtractor extends SafeTypeMatchingExtractor(classOf[com.google.cloud.spark.bigquery.DefaultSource])

  private def asSourceId(project: String, schema: String, table: String) = SourceIdentifier(Some("bigquery"), s"bigquery:$project: $schema :$table")

}
