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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.plans.logical._
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.commons.reflect.ReflectionUtils
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Params, Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DataSourceV2Plugin._
import za.co.absa.spline.harvester.plugin.{Plugin, ReadNodeProcessing, WriteNodeProcessing}

import java.net.URI
import javax.annotation.Priority
import scala.language.reflectiveCalls
import scala.util.Try

@Priority(Precedence.Normal)
class DataSourceV2Plugin
  extends Plugin
    with ReadNodeProcessing
    with WriteNodeProcessing {

  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case `_: DataSourceV2Relation`(relation) =>
      val table = extractValue[AnyRef](relation, "table")
      val tableName = extractValue[String](table, "name")
      val identifier = extractValue[AnyRef](relation, "identifier")
      val options = extractValue[AnyRef](relation, "options")
      val props = Map(
        "table" -> Map("identifier" -> tableName),
        "identifier" -> identifier,
        "options" -> options)
      ReadNodeInfo(extractSourceIdFromTable(table), props)
  }

  override val writeNodeProcessor: PartialFunction[(FuncName, LogicalPlan), WriteNodeInfo] = {
    case (_, `_: V2WriteCommand`(writeCommand)) =>
      val namedRelation = extractValue[AnyRef](writeCommand, "table")
      val query = extractValue[LogicalPlan](writeCommand, "query")

      val tableName = extractValue[AnyRef](namedRelation, "name")
      val output = extractValue[AnyRef](namedRelation, "output")
      val writeOptions = extractValue[Map[String, String]](writeCommand, "writeOptions")
      val isByName = extractValue[Boolean](writeCommand, IsByName)

      val props = Map(
        "table" -> Map("identifier" -> tableName, "output" -> output),
        "writeOptions" -> writeOptions,
        IsByName -> isByName)

      val sourceId = extractSourceIdFromRelation(namedRelation)

      processV2WriteCommand(writeCommand, sourceId, query, props)

    case (_, `_: CreateTableAsSelect`(ctc)) =>
      val prop = "ignoreIfExists" -> extractValue[Boolean](ctc, "ignoreIfExists")
      processV2CreateTableCommand(ctc, prop)

    case (_, `_: ReplaceTableAsSelect`(ctc)) =>
      val prop = "orCreate" -> extractValue[Boolean](ctc, "orCreate")
      processV2CreateTableCommand(ctc, prop)
  }

  /**
   * @param v2WriteCommand org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand
   */
  private def processV2WriteCommand(
    v2WriteCommand: AnyRef,
    sourceId: SourceIdentifier,
    query: LogicalPlan,
    props: Params
  ): WriteNodeInfo = v2WriteCommand match {
    case `_: AppendData`(_) =>
      WriteNodeInfo(sourceId, SaveMode.Append, query, props)

    case `_: OverwriteByExpression`(obe) =>
      val deleteExpr = extractValue[AnyRef](obe, "deleteExpr")
      WriteNodeInfo(sourceId, SaveMode.Overwrite, query, props + ("deleteExpr" -> deleteExpr))

    case `_: OverwritePartitionsDynamic`(_) =>
      WriteNodeInfo(sourceId, SaveMode.Overwrite, query, props)
  }

  private def processV2CreateTableCommand(
    ctc: AnyRef,
    commandSpecificProp: (String, _)
  ): WriteNodeInfo = {
    val catalog = extractCatalog(ctc)
    val identifier = extractValue[AnyRef](ctc, "tableName")
    val loadTableMethods = catalog.getClass.getMethods.filter(_.getName == "loadTable")
    val table = loadTableMethods.flatMap(m => Try(m.invoke(catalog, identifier)).toOption).head
    val sourceId = extractSourceIdFromTable(table)

    val query = extractValue[LogicalPlan](ctc, "query")

    val partitioning = extractValue[AnyRef](ctc, "partitioning")
    val properties = Try(extractValue[Map[String, String]](ctc, "properties")).getOrElse(Map.empty)
    val writeOptions = extractValue[Map[String, String]](ctc, "writeOptions")
    val props = Map(
      "table" -> Map("identifier" -> identifier.toString),
      "partitioning" -> partitioning,
      "properties" -> properties,
      "writeOptions" -> writeOptions)

    WriteNodeInfo(sourceId, SaveMode.Overwrite, query, props + commandSpecificProp)
  }

  private def extractCatalog(ctc: AnyRef): AnyRef = {
    Try {
      // Spark up to 3.2
      extractValue[AnyRef](ctc, "catalog")
    } getOrElse {
      // Spark 3.3+
      val name = extractValue[AnyRef](ctc, "name")
      extractValue[AnyRef](name, "catalog")
    }
  }


  /**
   * @param namedRelation org.apache.spark.sql.catalyst.analysis.NamedRelation
   */
  private def extractSourceIdFromRelation(namedRelation: AnyRef): SourceIdentifier = {
    val table = extractValue[AnyRef](namedRelation, "table")
    extractSourceIdFromTable(table)
  }

  /**
   * @param table org.apache.spark.sql.connector.catalog.Table
   */
  private def extractSourceIdFromTable(table: AnyRef): SourceIdentifier = table match {
    case `_: CassandraTable`(ct) =>
      val metadata = extractValue[AnyRef](ct, "metadata")
      val keyspace = extractValue[AnyRef](metadata, "keyspace")
      val name = extractValue[AnyRef](metadata, "name")
      SourceIdentifier(Some("cassandra"), s"cassandra:$keyspace:$name")

    case `_: DatabricksDeltaTableV2`(dt) => extractSourceIdFromDeltaTableV2(dt)

    case `_: FileTable`(ft) =>
      val format = extractValue[String](ft, "formatName").toLowerCase
      val paths = extractValue[Seq[String]](ft, "paths")
      val pathUris = paths.map(prependFileSchemaIfMissing)
      SourceIdentifier(Some(format), pathUris: _*)

    case `_: TableV2`(tv2) => extractSourceIdFromDeltaTableV2(tv2)

    case `_: OdpsTable`(ot) => SourceIdentifier(Some("odpstable"),s"odpstable:${extractValue[String](ot,"name")}")
  }

  private def extractSourceIdFromDeltaTableV2(table: AnyRef): SourceIdentifier = {
    val tableProps = extractValue[java.util.Map[String, String]](table, "properties")

    // for org.apache.spark.sql.delta.catalog.DeltaTableV2 delta v 1.2.0+
    val maybePath = Try(ReflectionUtils.extractValue[Path](table, "path")).toOption

    val location = maybePath
      .map(p => CatalogUtils.URIToString(p.toUri))
      .getOrElse(tableProps.get("location"))

    val uri =
      if (URI.create(location).getScheme == null) {
        s"file:$location"
      } else {
        location
      }
    val provider = tableProps.get("provider")
    SourceIdentifier(Some(provider), uri)
  }

  private def prependFileSchemaIfMissing(uri: String): String =
    if (URI.create(uri).getScheme == null) {
      s"file:$uri"
    } else {
      uri
    }

}

object DataSourceV2Plugin {
  val IsByName = "isByName"

  object `_: V2WriteCommand` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand")

  private object `_: AppendData` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.AppendData")

  private object `_: OverwriteByExpression` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression")

  private object `_: OverwritePartitionsDynamic` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic")

  private object `_: CreateTableAsSelect` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect")

  private object `_: ReplaceTableAsSelect` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect")

  private object `_: DataSourceV2Relation` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation")

  private object `_: FileTable` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.FileTable")

  private object `_: DatabricksDeltaTableV2` extends SafeTypeMatchingExtractor[AnyRef](
    "com.databricks.sql.transaction.tahoe.catalog.DeltaTableV2")

  private object `_: CassandraTable` extends SafeTypeMatchingExtractor[AnyRef](
    "com.datastax.spark.connector.datasource.CassandraTable")

  private object `_: TableV2` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.connector.catalog.Table")

  private object `_: OdpsTable` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.odps.OdpsTable")

}
