/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.builder.write

import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.commons.reflect.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.write.PluggableWriteCommandExtractor._
import za.co.absa.spline.harvester.builder.{DataSourceFormatNameResolver, SourceIdentifier, SourceUri}
import za.co.absa.spline.harvester.exception.UnsupportedSparkCommandException
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.PartialFunction.{cond, condOpt}
import scala.language.reflectiveCalls

class PluggableWriteCommandExtractor(pathQualifier: PathQualifier, session: SparkSession)
  extends WriteCommandExtractor {

  import za.co.absa.commons.ExtractorImplicits._

  @throws(classOf[UnsupportedSparkCommandException])
  def asWriteCommand(operation: LogicalPlan): Option[WriteCommand] = {
    val maybeWriteCommand = condOpt(operation) {

      // JDBC
      case cmd: SaveIntoDataSourceCommand
        if cond(cmd) { case DataSourceTypeExtractor(st) => st == "jdbc" || st.isInstanceOf[JdbcRelationProvider] } =>
        val jdbcConnectionString = cmd.options("url")
        val tableName = cmd.options("dbtable")
        WriteCommand(cmd.nodeName, SourceIdentifier.forJDBC(jdbcConnectionString, tableName), cmd.mode, cmd.query)

      // Cassandra
      case cmd: SaveIntoDataSourceCommand
        if cond(cmd) { case DataSourceTypeExtractor(st) => st == "org.apache.spark.sql.cassandra" || CassandraSourceExtractor.matches(st) } =>
        val keyspace = cmd.options("keyspace")
        val table = cmd.options("table")
        WriteCommand(cmd.nodeName, SourceIdentifier.forCassandra(keyspace, table), cmd.mode, cmd.query, cmd.options)

      // Mongo
      case cmd: SaveIntoDataSourceCommand
        if cond(cmd) { case DataSourceTypeExtractor(st) => st == "com.mongodb.spark.sql.DefaultSource" || MongoDBSourceExtractor.matches(st) } =>
        val database = cmd.options("database")
        val collection = cmd.options("collection")
        val uri = cmd.options("uri")
        WriteCommand(cmd.nodeName, SourceIdentifier.forMongoDB(uri, database, collection), cmd.mode, cmd.query, cmd.options)

      // ElasticSearch
      case cmd: SaveIntoDataSourceCommand
        if cond(cmd) { case DataSourceTypeExtractor(st) => st == "es" || ElasticSearchSourceExtractor.matches(st) } =>
        val indexDocType = cmd.options("path")
        val server = cmd.options("es.nodes")
        WriteCommand(cmd.nodeName, SourceIdentifier.forElasticSearch(server, indexDocType), cmd.mode, cmd.query, cmd.options)

      // Kafka
      case cmd: SaveIntoDataSourceCommand
        if cmd.options.contains("kafka.bootstrap.servers") =>
        val maybeFormat = DataSourceTypeExtractor.unapply(cmd).map(DataSourceFormatNameResolver.resolve)
        val uri = SourceUri.forKafka(cmd.options("topic"))
        WriteCommand(cmd.nodeName, SourceIdentifier(maybeFormat, uri), cmd.mode, cmd.query, cmd.options)

      // FS

      case cmd: SaveIntoDataSourceCommand =>
        val maybeFormat = DataSourceTypeExtractor.unapply(cmd).map(DataSourceFormatNameResolver.resolve)
        val opts = cmd.options
        val uri = opts.get("path").map(pathQualifier.qualify)
          .getOrElse(sys.error(s"Cannot extract source URI from the options: ${opts.keySet mkString ","}"))
        WriteCommand(cmd.nodeName, SourceIdentifier(maybeFormat, uri), cmd.mode, cmd.query, opts)

      // SQL... ???

      case cmd: InsertIntoHadoopFsRelationCommand if cmd.catalogTable.isDefined =>
        val catalogTable = cmd.catalogTable.get
        val mode = if (cmd.mode == SaveMode.Overwrite) Overwrite else Append
        asTableWriteCommand(cmd.nodeName, catalogTable, mode, cmd.query)

      case cmd: InsertIntoHadoopFsRelationCommand =>
        val path = cmd.outputPath.toString
        val qPath = pathQualifier.qualify(path)
        val format = DataSourceFormatNameResolver.resolve(cmd.fileFormat)
        WriteCommand(cmd.nodeName, SourceIdentifier(Some(format), qPath), cmd.mode, cmd.query, cmd.options)

      case cmd: InsertIntoDataSourceCommand =>
        val catalogTable = cmd.logicalRelation.catalogTable
        val path = catalogTable.flatMap(_.storage.locationUri).map(_.toString)
          .getOrElse(sys.error(s"Cannot extract source URI from InsertIntoDataSourceCommand"))
        val format = catalogTable.flatMap(_.provider).map(_.toLowerCase)
          .getOrElse(sys.error(s"Cannot extract format from InsertIntoDataSourceCommand"))
        val qPath = pathQualifier.qualify(path)
        val mode = if (cmd.overwrite) SaveMode.Overwrite else SaveMode.Append
        WriteCommand(cmd.nodeName, SourceIdentifier(Some(format), qPath), mode, cmd.query)

      case `_: InsertIntoDataSourceDirCommand`(cmd) =>
        asDirWriteCommand(cmd.nodeName, cmd.storage, cmd.provider, cmd.overwrite, cmd.query)

      case `_: InsertIntoHiveDirCommand`(cmd) =>
        asDirWriteCommand(cmd.nodeName, cmd.storage, "hive", cmd.overwrite, cmd.query)

      case `_: InsertIntoHiveTable`(cmd) =>
        val mode = if (cmd.overwrite) Overwrite else Append
        asTableWriteCommand(cmd.nodeName, cmd.table, mode, cmd.query)

      case `_: CreateHiveTableAsSelectCommand`(cmd) =>
        val sourceId = SourceIdentifier.forTable(cmd.tableDesc)(pathQualifier, session)
        WriteCommand(cmd.nodeName, sourceId, cmd.mode, cmd.query)

      case cmd: CreateDataSourceTableAsSelectCommand =>
        asTableWriteCommand(cmd.nodeName, cmd.table, cmd.mode, cmd.query)

      case dtc: DropTableCommand =>
        val uri = SourceUri.forTable(dtc.tableName)(session)
        val sourceId = SourceIdentifier(None, pathQualifier.qualify(uri))
        WriteCommand(dtc.nodeName, sourceId, Overwrite, dtc)

      case ctc: CreateTableCommand =>
        val sourceId = SourceIdentifier.forTable(ctc.table)(pathQualifier, session)
        WriteCommand(ctc.nodeName, sourceId, Overwrite, ctc)
    }

    if (maybeWriteCommand.isEmpty) alertWhenUnimplementedCommand(operation)

    maybeWriteCommand
  }

  private def asDirWriteCommand(name: String, storage: CatalogStorageFormat, provider: String, overwrite: Boolean, query: LogicalPlan) = {
    val uri = storage.locationUri.getOrElse(sys.error(s"Cannot determine the data source location: $storage"))
    val mode = if (overwrite) Overwrite else Append
    WriteCommand(name, SourceIdentifier(Some(provider), uri.toString), mode, query)
  }

  private def asTableWriteCommand(name: String, table: CatalogTable, mode: SaveMode, query: LogicalPlan) = {
    val sourceIdentifier = SourceIdentifier.forTable(table)(pathQualifier, session)
    WriteCommand(name, sourceIdentifier, mode, query, Map("table" -> Map("identifier" -> table.identifier, "storage" -> table.storage)))
  }

  private val commandsToBeImplemented = Seq(
    classOf[AlterTableAddColumnsCommand],
    classOf[AlterTableChangeColumnCommand],
    classOf[AlterTableRenameCommand],
    classOf[AlterTableSetLocationCommand],
    classOf[CreateDataSourceTableCommand],
    classOf[CreateDatabaseCommand],
    classOf[CreateTableLikeCommand],
    classOf[DropDatabaseCommand],
    classOf[LoadDataCommand],
    classOf[TruncateTableCommand]
  )

  private def alertWhenUnimplementedCommand(c: LogicalPlan): Unit = {
    if (commandsToBeImplemented.contains(c.getClass)) throw new UnsupportedSparkCommandException(c)
  }
}

object PluggableWriteCommandExtractor {

  private object `_: InsertIntoHiveTable` extends SafeTypeMatchingExtractor(classOf[InsertIntoHiveTable])

  private object `_: CreateHiveTableAsSelectCommand` extends SafeTypeMatchingExtractor(classOf[CreateHiveTableAsSelectCommand])

  private object `_: InsertIntoHiveDirCommand` extends SafeTypeMatchingExtractor[InsertIntoHiveDirCommand]("org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand")

  private object `_: InsertIntoDataSourceDirCommand` extends SafeTypeMatchingExtractor[InsertIntoDataSourceDirCommand]("org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand")

  private object CassandraSourceExtractor extends SafeTypeMatchingExtractor(classOf[org.apache.spark.sql.cassandra.DefaultSource])

  private object MongoDBSourceExtractor extends SafeTypeMatchingExtractor(classOf[com.mongodb.spark.sql.DefaultSource])

  private object ElasticSearchSourceExtractor extends SafeTypeMatchingExtractor(classOf[org.elasticsearch.spark.sql.DefaultSource15])

  private object DataSourceTypeExtractor extends AccessorMethodValueExtractor[AnyRef]("provider", "dataSource")

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
