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

package za.co.absa.spline.harvester.builder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import za.co.absa.spline.harvester.qualifier.PathQualifier

object BuilderUtils {

  def extractCatalogTableParams(catalogTable: CatalogTable): Map[String, Any] = {
    Map("table" -> Map(
      "identifier" -> catalogTable.identifier,
      "storage" -> catalogTable.storage))
  }
}

object SourceId {
  def forKafka(topics: String*): SourceIdentifier =
    SourceIdentifier(Some("kafka"), topics.map(SourceUri.forKafka): _*)

  def forJDBC(connectionUrl: String, table: String): SourceIdentifier =
    SourceIdentifier(Some("jdbc"), SourceUri.forJDBC(connectionUrl, table))

  def forTable(table: CatalogTable)
    (pathQualifier: PathQualifier, session: SparkSession): SourceIdentifier = {
    val uri = table.storage.locationUri
      .map(_.toString)
      .getOrElse(SourceUri.forTable(table.identifier)(session))
    SourceIdentifier(table.provider, pathQualifier.qualify(uri))
  }

  def forExcel(filePath: String): SourceIdentifier =
    SourceIdentifier(Some("excel"), filePath)

  def forCobol(filePath: String): SourceIdentifier =
    SourceIdentifier(Some("cobol"), filePath)

  def forCassandra(keyspace: String, table: String): SourceIdentifier =
    SourceIdentifier(Some("cassandra"), SourceUri.forCassandra(keyspace, table))

  def forMongoDB(connectionUrl: String, database: String, collection: String): SourceIdentifier =
    SourceIdentifier(Some("mongodb"), SourceUri.forMongoDB(connectionUrl, database, collection))

  def forElasticSearch(server: String, indexDocType: String): SourceIdentifier =
    SourceIdentifier(Some("elasticsearch"), SourceUri.forElasticSearch(server, indexDocType))

  def forXml(qualifiedPaths: Seq[String]): SourceIdentifier =
    SourceIdentifier(Some("xml"), qualifiedPaths: _*)

}

object SourceUri {
  def forKafka(topic: String): String = s"kafka:$topic"

  def forJDBC(connectionUrl: String, table: String): String = s"$connectionUrl:$table"

  def forCassandra(keyspace: String, table: String): String = s"cassandra:$keyspace:$table"

  def forMongoDB(connectionUrl: String, database: String, collection: String): String = s"$connectionUrl/$database.$collection"

  def forElasticSearch(server: String, indexDocType: String): String = s"elasticsearch://$server/$indexDocType"

  def forTable(tableIdentifier: TableIdentifier)
    (session: SparkSession): String = {
    val catalog = session.catalog
    val TableIdentifier(tableName, maybeTableDatabase) = tableIdentifier
    val databaseName = maybeTableDatabase getOrElse catalog.currentDatabase
    val databaseLocation = catalog.getDatabase(databaseName).locationUri.stripSuffix("/")
    s"$databaseLocation/${tableName.toLowerCase}"
  }
}
