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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Params, Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DataSourceV2Plugin._
import za.co.absa.spline.harvester.plugin.{Plugin, ReadNodeProcessing, WriteNodeProcessing}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import javax.annotation.Priority
import scala.language.reflectiveCalls
import scala.util.{Success, Try}

@Priority(Precedence.Normal)
class DataSourceV2Plugin(
  pathQualifier: PathQualifier,
  session: SparkSession)
  extends Plugin
    with ReadNodeProcessing
    with WriteNodeProcessing {

  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case `_: DataSourceV2Relation`(relation) =>
      val table = extractFieldValue[AnyRef](relation, "table")
      val tableName = extractFieldValue[String](table, "name")
      val identifier = extractFieldValue[AnyRef](relation, "identifier")
      val options =  extractFieldValue[AnyRef](relation, "options")
      val props = Map(
        "table" -> Map("identifier" -> tableName),
        "identifier" -> identifier,
        "options" -> options)
      (extractSourceIdFromTable(table), props)
  }

  override val writeNodeProcessor: PartialFunction[LogicalPlan, WriteNodeInfo] = {
    case `_: V2WriteCommand`(writeCommand) => {
      val namedRelation = extractFieldValue[AnyRef](writeCommand, "table")
      val query = extractFieldValue[LogicalPlan](writeCommand, "query")

      val tableName = extractFieldValue[AnyRef](namedRelation, "name")
      val writeOptions = extractFieldValue[Map[String, String]](writeCommand, "writeOptions")
      val isByName = extractFieldValue[Boolean](writeCommand, "isByName")

      val props = Map(
        "table" -> Map("identifier" -> tableName),
        "writeOptions" -> writeOptions,
        "isByName" -> isByName)

      val sourceId = extractSourceIdFromRelation(namedRelation)

      processV2WriteCommand(writeCommand, sourceId, query, props)
    }

    case `_: V2CreateTablePlan`(ctp) => {
      val catalog = extractFieldValue[AnyRef](ctp, "catalog")
      val identifier = extractFieldValue[AnyRef](ctp, "tableName")
      val loadTableMethods = catalog.getClass.getMethods.filter(_.getName == "loadTable")
      val table = loadTableMethods.flatMap(m => Try(m.invoke(catalog, identifier)).toOption).head
      val sourceId = extractSourceIdFromTable(table)

      val query = extractFieldValue[LogicalPlan](ctp, "query")

      val partitioning = extractFieldValue[AnyRef](ctp, "partitioning")
      val properties = extractFieldValue[Map[String, String]](ctp, "properties")
      val writeOptions = extractFieldValue[Map[String, String]](ctp, "writeOptions")
      val props = Map(
        "table" -> Map("identifier" -> identifier.toString),
        "partitioning" -> partitioning,
        "properties" -> properties,
        "writeOptions" -> writeOptions)

      val commandSpecificProp = ctp match {
        case `_: CreateTableAsSelect`(_) =>
          "ignoreIfExists" -> extractFieldValue[Boolean](ctp, "ignoreIfExists")
        case `_: ReplaceTableAsSelect`(_) =>
          "orCreate" -> extractFieldValue[Boolean](ctp, "orCreate")
      }

      (sourceId, SaveMode.Overwrite, query, props + commandSpecificProp)
    }
  }

  /**
    * @param v2WriteCommand org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand
    */
  private def processV2WriteCommand(
    v2WriteCommand: AnyRef,
    sourceId: SourceIdentifier,
    query: LogicalPlan,
    props: Params
  ): (SourceIdentifier, SaveMode, LogicalPlan, Params) = v2WriteCommand match {
    case `_: AppendData`(_) =>
      (sourceId, SaveMode.Append, query, props)

    case `_: OverwriteByExpression`(obe) =>
      val deleteExpr = extractFieldValue[AnyRef](obe, "deleteExpr")
      (sourceId, SaveMode.Overwrite, query, props + ("deleteExpr" -> deleteExpr.toString))

    case `_: OverwritePartitionsDynamic`(_) =>
      (sourceId, SaveMode.Overwrite, query, props)
  }

  /**
    * @param namedRelation org.apache.spark.sql.catalyst.analysis.NamedRelation
    */
  private def extractSourceIdFromRelation(namedRelation: AnyRef): SourceIdentifier = {
    val table = extractFieldValue[AnyRef](namedRelation, "table")
    extractSourceIdFromTable(table)
  }

  /**
    * @param table org.apache.spark.sql.connector.catalog.Table
    */
  private def extractSourceIdFromTable(table: AnyRef): SourceIdentifier = table match {
    case `_: CassandraTable`(ct) =>
      val metadata = extractFieldValue[AnyRef](ct, "metadata")
      val keyspace = extractFieldValue[AnyRef](metadata, "keyspace")
      val name = extractFieldValue[AnyRef](metadata, "name")
      SourceIdentifier(Some("cassandra"), s"cassandra:$keyspace:$name")

    case `_: DeltaTableV2`(dt) => extractSourceIdFromDeltaTableV2(dt)
    case `_: DatabricksDeltaTableV2`(dt) => extractSourceIdFromDeltaTableV2(dt)

    case `_: FileTable`(ft) =>
      val format = extractFieldValue[String](ft, "formatName").toLowerCase
      val paths = extractFieldValue[Seq[String]](ft, "paths")
      SourceIdentifier(Some(format), paths: _*)
  }

  private def extractSourceIdFromDeltaTableV2(table: AnyRef): SourceIdentifier = {
    val tableProps = extractFieldValue[java.util.Map[String, String]](table, "properties")
    val uri = tableProps.get("location")
    val provider = tableProps.get("provider")
    SourceIdentifier(Some(provider), uri)
  }

}

object DataSourceV2Plugin {
  object `_: V2WriteCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand")

  object `_: AppendData` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.AppendData")

  object `_: OverwriteByExpression` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression")

  object `_: OverwritePartitionsDynamic` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic")

  object `_: V2CreateTablePlan` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.V2CreateTablePlan")

  object `_: CreateTableAsSelect` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect")

  object `_: ReplaceTableAsSelect` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect")

  object `_: DataSourceV2Relation` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation")

  object `_: FileTable` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.FileTable")

  object `_: DeltaTableV2` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.delta.catalog.DeltaTableV2")

  object `_: DatabricksDeltaTableV2` extends SafeTypeMatchingExtractor[AnyRef](
    "com.databricks.sql.transaction.tahoe.catalog.DeltaTableV2")

  object `_: CassandraTable` extends SafeTypeMatchingExtractor[AnyRef](
    "com.datastax.spark.connector.datasource.CassandraTable")

}
