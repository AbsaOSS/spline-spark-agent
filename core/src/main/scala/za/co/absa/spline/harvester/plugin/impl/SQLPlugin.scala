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

import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateTableCommand, DropTableCommand}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.impl.SQLPlugin._
import za.co.absa.spline.harvester.plugin.{Plugin, ReadPlugin, WritePlugin}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.language.reflectiveCalls

class SQLPlugin(pathQualifier: PathQualifier, session: SparkSession)
  extends Plugin
    with ReadPlugin
    with WritePlugin {

  override val readNodeProcessor: PartialFunction[LogicalPlan, (SourceIdentifier, Params)] = {
    case htr: HiveTableRelation =>
      asTableRead(htr.tableMeta)

    case lr: LogicalRelation if lr.relation.isInstanceOf[HadoopFsRelation] =>
      val hr = lr.relation.asInstanceOf[HadoopFsRelation]
      lr.catalogTable
        .map(asTableRead)
        .getOrElse {
          val uris = hr.location.rootPaths.map(path => pathQualifier.qualify(path.toString))
          val fileFormat = hr.fileFormat
          val formatName = DataSourceFormatResolver.resolve(fileFormat)
          (SourceIdentifier(Some(formatName), uris: _*), hr.options)
        }
  }

  override val writeNodeProcessor: PartialFunction[LogicalPlan, (SourceIdentifier, SaveMode, LogicalPlan, Params)] = {

    case cmd: InsertIntoHadoopFsRelationCommand if cmd.catalogTable.isDefined =>
      val catalogTable = cmd.catalogTable.get
      val mode = if (cmd.mode == SaveMode.Overwrite) Overwrite else Append
      asTableWrite(catalogTable, mode, cmd.query)

    case cmd: InsertIntoHadoopFsRelationCommand =>
      val path = cmd.outputPath.toString
      val qPath = pathQualifier.qualify(path)
      val format = DataSourceFormatResolver.resolve(cmd.fileFormat)
      (SourceIdentifier(Some(format), qPath), cmd.mode, cmd.query, cmd.options)

    case cmd: InsertIntoDataSourceCommand =>
      val catalogTable = cmd.logicalRelation.catalogTable
      val path = catalogTable.flatMap(_.storage.locationUri).map(_.toString)
        .getOrElse(sys.error(s"Cannot extract source URI from InsertIntoDataSourceCommand"))
      val format = catalogTable.flatMap(_.provider).map(_.toLowerCase)
        .getOrElse(sys.error(s"Cannot extract format from InsertIntoDataSourceCommand"))
      val qPath = pathQualifier.qualify(path)
      val mode = if (cmd.overwrite) SaveMode.Overwrite else SaveMode.Append
      (SourceIdentifier(Some(format), qPath), mode, cmd.query, Map.empty)

    case `_: InsertIntoDataSourceDirCommand`(cmd) =>
      asDirWrite(cmd.storage, cmd.provider, cmd.overwrite, cmd.query)

    case `_: InsertIntoHiveDirCommand`(cmd) =>
      asDirWrite(cmd.storage, "hive", cmd.overwrite, cmd.query)

    case `_: InsertIntoHiveTable`(cmd) =>
      val mode = if (cmd.overwrite) Overwrite else Append
      asTableWrite(cmd.table, mode, cmd.query)

    case `_: CreateHiveTableAsSelectCommand`(cmd) =>
      val sourceId = asTableSourceId(cmd.tableDesc)
      (sourceId, cmd.mode, cmd.query, Map.empty)

    case cmd: CreateDataSourceTableAsSelectCommand =>
      asTableWrite(cmd.table, cmd.mode, cmd.query)

    case dtc: DropTableCommand =>
      val uri = asTableURI(dtc.tableName)
      val sourceId = SourceIdentifier(None, pathQualifier.qualify(uri))
      (sourceId, Overwrite, dtc, Map.empty)

    case ctc: CreateTableCommand =>
      val sourceId = asTableSourceId(ctc.table)
      (sourceId, Overwrite, ctc, Map.empty)
  }

  private def asTableURI(tableIdentifier: TableIdentifier): String = {
    val catalog = session.catalog
    val TableIdentifier(tableName, maybeTableDatabase) = tableIdentifier
    val databaseName = maybeTableDatabase getOrElse catalog.currentDatabase
    val databaseLocation = catalog.getDatabase(databaseName).locationUri.stripSuffix("/")
    s"$databaseLocation/${tableName.toLowerCase}"
  }

  private def asTableSourceId(table: CatalogTable): SourceIdentifier = {
    val uri = table.storage.locationUri
      .map(_.toString)
      .getOrElse(asTableURI(table.identifier))
    SourceIdentifier(table.provider, pathQualifier.qualify(uri))
  }

  private def asTableRead(ct: CatalogTable) = {
    val sourceId = asTableSourceId(ct)
    val params = Map(
      "table" -> Map(
        "identifier" -> ct.identifier,
        "storage" -> ct.storage))
    (sourceId, params)
  }

  private def asTableWrite(table: CatalogTable, mode: SaveMode, query: LogicalPlan) = {
    val sourceIdentifier = asTableSourceId(table)
    (sourceIdentifier, mode, query, Map("table" -> Map("identifier" -> table.identifier, "storage" -> table.storage)))
  }

  private def asDirWrite(storage: CatalogStorageFormat, provider: String, overwrite: Boolean, query: LogicalPlan) = {
    val uri = storage.locationUri.getOrElse(sys.error(s"Cannot determine the data source location: $storage"))
    val mode = if (overwrite) Overwrite else Append
    (SourceIdentifier(Some(provider), uri.toString), mode, query, Map.empty: Params)
  }
}

object SQLPlugin {

  private object `_: InsertIntoHiveTable` extends SafeTypeMatchingExtractor(classOf[InsertIntoHiveTable])

  private object `_: CreateHiveTableAsSelectCommand` extends SafeTypeMatchingExtractor(classOf[CreateHiveTableAsSelectCommand])

  private object `_: InsertIntoHiveDirCommand` extends SafeTypeMatchingExtractor[InsertIntoHiveDirCommand]("org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand")

  private object `_: InsertIntoDataSourceDirCommand` extends SafeTypeMatchingExtractor[InsertIntoDataSourceDirCommand]("org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand")

  private type InsertIntoHiveDirCommand = {
    def storage: CatalogStorageFormat
    def query: LogicalPlan
    def overwrite: Boolean
    def nodeName: String
  }

  private type InsertIntoDataSourceDirCommand = {
    def storage: CatalogStorageFormat
    def provider: String
    def query: LogicalPlan
    def overwrite: Boolean
    def nodeName: String
  }
}
