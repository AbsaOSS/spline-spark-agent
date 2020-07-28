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
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateTableCommand, DropTableCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
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
      val catalogTable = htr.tableMeta
      val sourceId = SourceId.forTable(catalogTable)(pathQualifier, session)
      val params = BuilderUtils.extractCatalogTableParams(catalogTable)
      (sourceId, params)
  }

  override val writeNodeProcessor: PartialFunction[LogicalPlan, (SourceIdentifier, SaveMode, LogicalPlan, Params)] = {

    case cmd: InsertIntoHadoopFsRelationCommand if cmd.catalogTable.isDefined =>
      val catalogTable = cmd.catalogTable.get
      val mode = if (cmd.mode == SaveMode.Overwrite) Overwrite else Append
      asTableWriteCommand(catalogTable, mode, cmd.query)

    case cmd: InsertIntoHadoopFsRelationCommand =>
      val path = cmd.outputPath.toString
      val qPath = pathQualifier.qualify(path)
      val format = DataSourceFormatNameResolver.resolve(cmd.fileFormat)
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
      asDirWriteCommand(cmd.storage, cmd.provider, cmd.overwrite, cmd.query)

    case `_: InsertIntoHiveDirCommand`(cmd) =>
      asDirWriteCommand(cmd.storage, "hive", cmd.overwrite, cmd.query)

    case `_: InsertIntoHiveTable`(cmd) =>
      val mode = if (cmd.overwrite) Overwrite else Append
      asTableWriteCommand(cmd.table, mode, cmd.query)

    case `_: CreateHiveTableAsSelectCommand`(cmd) =>
      val sourceId = SourceId.forTable(cmd.tableDesc)(pathQualifier, session)
      (sourceId, cmd.mode, cmd.query, Map.empty)

    case cmd: CreateDataSourceTableAsSelectCommand =>
      asTableWriteCommand(cmd.table, cmd.mode, cmd.query)

    case dtc: DropTableCommand =>
      val uri = SourceUri.forTable(dtc.tableName)(session)
      val sourceId = SourceIdentifier(None, pathQualifier.qualify(uri))
      (sourceId, Overwrite, dtc, Map.empty)

    case ctc: CreateTableCommand =>
      val sourceId = SourceId.forTable(ctc.table)(pathQualifier, session)
      (sourceId, Overwrite, ctc, Map.empty)
  }

  private def asDirWriteCommand(storage: CatalogStorageFormat, provider: String, overwrite: Boolean, query: LogicalPlan) = {
    val uri = storage.locationUri.getOrElse(sys.error(s"Cannot determine the data source location: $storage"))
    val mode = if (overwrite) Overwrite else Append
    (SourceIdentifier(Some(provider), uri.toString), mode, query, Map.empty: Params)
  }

  private def asTableWriteCommand(table: CatalogTable, mode: SaveMode, query: LogicalPlan) = {
    val sourceIdentifier = SourceId.forTable(table)(pathQualifier, session)
    (sourceIdentifier, mode, query, Map("table" -> Map("identifier" -> table.identifier, "storage" -> table.storage)))
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
