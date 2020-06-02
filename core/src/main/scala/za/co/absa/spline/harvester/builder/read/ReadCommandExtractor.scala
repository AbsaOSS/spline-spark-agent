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

package za.co.absa.spline.harvester.builder.read

import java.io.InputStream
import java.util.Properties

import com.crealytics.spark.excel.{ExcelRelation, WorkbookReader}
import com.databricks.spark.xml.XmlRelation
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.MongoRDD
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.TableRef
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.kafka010.{AssignStrategy, ConsumerStrategy, SubscribePatternStrategy, SubscribeStrategy}
import org.apache.spark.sql.sources.BaseRelation
import org.elasticsearch.spark.cfg.SparkSettings
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.builder.read.ReadCommandExtractor._
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.PartialFunction.condOpt
import scala.collection.JavaConverters._
import scala.util.Try


class ReadCommandExtractor(pathQualifier: PathQualifier, session: SparkSession) {
  def asReadCommand(operation: LogicalPlan): Option[ReadCommand] =
    condOpt(operation) {
      case lr: LogicalRelation => lr.relation match {
        case hr: HadoopFsRelation =>
          lr.catalogTable
            .map(catalogTable => {
              ReadCommand(SourceIdentifier.forTable(catalogTable)(pathQualifier, session), operation, params = extractCatalogTableParams(catalogTable))
            })
            .getOrElse({
              val uris = hr.location.rootPaths.map(path => pathQualifier.qualify(path.toString))
              val fileFormat = hr.fileFormat
              fileFormat match {
                case SparkAvroSourceRelation(_) =>
                  ReadCommand(SourceIdentifier(Some("Avro"), uris: _*), operation, hr.options)
                case DatabricksAvroSourceRelation(_) =>
                  ReadCommand(SourceIdentifier(Some("Avro"), uris: _*), operation, hr.options)
                case _ =>
                  val format = fileFormat.toString
                  ReadCommand(SourceIdentifier(Some(format), uris: _*), operation, hr.options)
              }
            })

        case xr: XmlRelation =>
          val uris = xr.location.toSeq.map(pathQualifier.qualify)
          ReadCommand(SourceIdentifier(Some("XML"), uris: _*), operation, xr.parameters)

        case `_: JDBCRelation`(jr) =>
          val jdbcOptions = extractFieldValue[JDBCOptions](jr, "jdbcOptions")
          val url = extractFieldValue[String](jdbcOptions, "url")
          val params = extractFieldValue[Map[String, String]](jdbcOptions, "parameters")
          val TableOrQueryFromJDBCOptionsExtractor(toq) = jdbcOptions
          ReadCommand(SourceIdentifier.forJDBC(url, toq), operation, params)

        case `_: KafkaRelation`(kr) =>
          val options = extractFieldValue[Map[String, String]](kr, "sourceOptions")
          val topics: Seq[String] = extractFieldValue[ConsumerStrategy](kr, "strategy") match {
            case AssignStrategy(partitions) => partitions.map(_.topic)
            case SubscribeStrategy(topics) => topics
            case SubscribePatternStrategy(pattern) => kafkaTopics(options("kafka.bootstrap.servers")).filter(_.matches(pattern))
          }
          ReadCommand(SourceIdentifier.forKafka(topics: _*), operation, options ++ Map(
            "startingOffsets" -> extractFieldValue[AnyRef](kr, "startingOffsets"),
            "endingOffsets" -> extractFieldValue[AnyRef](kr, "endingOffsets")
          ))

        case `_: ExcelRelation`(exr) =>
          val excelRelation = exr.asInstanceOf[ExcelRelation]
          val inputStream = extractExcelInputStream(excelRelation.workbookReader)
          val path = extractFieldValue[org.apache.hadoop.fs.Path](inputStream, "file")
          val qualifiedPath = pathQualifier.qualify(path.toString)
          ReadCommand(SourceIdentifier.forExcel(qualifiedPath), operation,
            extractExcelParams(excelRelation) + ("header" -> excelRelation.header.toString))

        case `_: CassandraSourceRelation`(casr) =>
          val tableRef = extractFieldValue[TableRef](casr, "tableRef")
          val table = tableRef.table
          val keyspace = tableRef.keyspace
          ReadCommand(SourceIdentifier.forCassandra(keyspace, table), operation)

        case `_: MongoDBSourceRelation`(mongr) =>
          val mongoRDD = extractFieldValue[MongoRDD[_]](mongr, "mongoRDD")
          val readConfig = extractFieldValue[ReadConfig](mongoRDD, "readConfig")
          val database = readConfig.databaseName
          val collection = readConfig.collectionName
          val connectionUrl = readConfig.connectionString.getOrElse(sys.error("Unable to extract MongoDB connection URL"))

          ReadCommand(SourceIdentifier.forMongoDB(connectionUrl, database, collection), operation)

        case `_: ElasticSearchSourceRelation`(esr) =>
          val parameters = extractFieldValue[SparkSettings](esr, "cfg")
          val server = parameters.getProperty("es.nodes")
          val indexDocType = parameters.getProperty("es.resource")

          ReadCommand(SourceIdentifier.forElasticSearch(server, indexDocType), operation)

        case `_: CobrixSourceRelation`(cobrix) =>
          val sourceDir = extractFieldValue[String](cobrix, "sourceDir")
          ReadCommand(SourceIdentifier.forCobrix(sourceDir), operation)

        case br: BaseRelation =>
          sys.error(s"Relation is not supported: $br")
      }

      case htr: HiveTableRelation =>
        val catalogTable = htr.tableMeta
        ReadCommand(SourceIdentifier.forTable(catalogTable)(pathQualifier, session), operation, params = extractCatalogTableParams(catalogTable))
    }
}

object ReadCommandExtractor {

  object `_: JDBCRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation")

  object `_: KafkaRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.kafka010.KafkaRelation")

  object `_: ExcelRelation` extends SafeTypeMatchingExtractor[AnyRef]("com.crealytics.spark.excel.ExcelRelation")

  object `_: CassandraSourceRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.cassandra.CassandraSourceRelation")

  object `_: MongoDBSourceRelation` extends SafeTypeMatchingExtractor[AnyRef]("com.mongodb.spark.sql.MongoRelation")

  object `_: ElasticSearchSourceRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.elasticsearch.spark.sql.ElasticsearchRelation")

  object SparkAvroSourceRelation extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.avro.AvroFileFormat")

  object DatabricksAvroSourceRelation extends SafeTypeMatchingExtractor[AnyRef]("com.databricks.spark.avro.DefaultSource")

  object `_: CobrixSourceRelation` extends SafeTypeMatchingExtractor[AnyRef]("za.co.absa.cobrix.spark.cobol.source.CobolRelation")

  object TableOrQueryFromJDBCOptionsExtractor extends AccessorMethodValueExtractor[String]("table", "tableOrQuery")

  private def kafkaTopics(bootstrapServers: String): Seq[String] = {
    val kc = new KafkaConsumer(new Properties {
      put("bootstrap.servers", bootstrapServers)
      put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    })
    try kc.listTopics.keySet.asScala.toSeq
    finally kc.close()
  }

  private def extractExcelInputStream(reader: WorkbookReader) = {

    val streamFieldName_Scala_2_12 = "inputStreamProvider"
    val streamFieldName_Scala_2_11_default = "com$crealytics$spark$excel$DefaultWorkbookReader$$inputStreamProvider"
    val streamFieldName_Scala_2_11_streaming = "com$crealytics$spark$excel$StreamingWorkbookReader$$inputStreamProvider"

    def extract(fieldName: String) = extractFieldValue[() => InputStream](reader, fieldName)

    val lazyStream = Try(extract(streamFieldName_Scala_2_12))
      .orElse(Try(extract(streamFieldName_Scala_2_11_default)))
      .orElse(Try(extract(streamFieldName_Scala_2_11_streaming)))
      .getOrElse(sys.error("Unable to extract Excel input stream"))

    lazyStream.apply()
  }

  private def extractExcelParams(excelRelation: ExcelRelation): Map[String, Any] = {
    val locator = excelRelation.dataLocator

    def extract(fieldName: String) =
      Try(extractFieldValue[Any](locator, fieldName))
        .map(_.toString)
        .getOrElse("")

    val fieldNames = locator.getClass.getDeclaredFields.map(_.getName)

    fieldNames.map(fn => fn -> extract(fn)).toMap
  }

  private def extractCatalogTableParams(catalogTable: CatalogTable): Map[String, Any] = {
    Map("table" -> Map(
      "identifier" -> catalogTable.identifier,
      "storage" -> catalogTable.storage))
  }

}
