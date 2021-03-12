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

package za.co.absa.spline

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture2

//TODO
/*
org.cassandraunit:cassandra-unit:4.3.1.0
com.datastax.spark:spark-cassandra-connector_${scala.binary.version}:3.0.0

called from spark-cassandra-connector: 2.4.2
A needed class was not found. This could be due to an error in your runpath. Missing class: com/codahale/metrics/JmxReporter

Maven: io.dropwizard.metrics:metrics-core:3.1.5
dropwizard / metrics : 3.1.1 - com.codahale.metrics.JmxReporter
                       4.1.1 - com.codahale.metrics.jmx.JmxReporter

 */
//class CassandraSpec2
//  extends AsyncFlatSpec
//    with Matchers
//    with SparkFixture
//    with SplineFixture2 {
//
//  it should "support Cassandra as a write source" in
//    withNewSparkSession { implicit spark =>
//      withLineageTracking { captor =>
//
//        //spark.conf.set("spark.cassandra.connection.port", "9142") //non default port for embedded cassandra
//
//        val keyspace = "test_keyspace"
//        val table = "test_table"
//
//        //Embedded Cassandra setup
//        EmbeddedCassandraServerHelper.startEmbeddedCassandra()
//        val session = EmbeddedCassandraServerHelper.getSession
//        val port = EmbeddedCassandraServerHelper.getNativeTransportPort
//
//
//        import com.datastax.spark.connector.cql.CassandraConnectorConf
//        import org.apache.spark.sql.cassandra._
//
//        // set params for all clusters and keyspaces
//        spark.setCassandraConf(CassandraConnectorConf.ConnectionPortParam.option(port))
//
//        //spark.conf.set("spark.cassandra.connection.port", port)
//
//        //val cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build()
//
//        session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace  WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
//        session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (ID INT, NAME TEXT, PRIMARY KEY (ID))")
//
//        spark.conf.set(s"spark.sql.catalog.cass100", "com.datastax.spark.connector.datasource.CassandraCatalog")
//        spark.conf.set(s"spark.sql.catalog.cass100.spark.cassandra.connection.port", port)
//
//        val testData: DataFrame = {
//          val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
//          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
//          spark.sqlContext.createDataFrame(rdd, schema)
//        }
//
//        //        for {
//        ////          (plan1, _) <- captor.lineageOf(testData
//        ////          .writeTo(s"cass100.$keyspace.$table")
//        ////            .append())
//        //
//        ////          .mode(SaveMode.Overwrite)
//        ////          .format("org.apache.spark.sql.cassandra")
//        ////          .options(Map("table" -> table, "keyspace" -> keyspace, "confirm.truncate" -> "true"))
//        ////          .save())
//        //
//        //          (plan2, _) <- captor.lineageOf {
//        //            testData
//        //              .writeTo(s"cass100.$keyspace.$table")
//        //              .append()
//        //
//        //            val df = spark
//        //              .read
//        //              .table(s"cass100.$keyspace.$table")
//        ////              .format("org.apache.spark.sql.cassandra")
//        ////              .options(Map("table" -> table, "keyspace" -> keyspace))
//        ////              .load()
//        //
//        //            df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
//        //          }
//        //        } yield {
//        //
//        ////          plan1.operations.write.append shouldBe false
//        ////          plan1.operations.write.extra.get("destinationType") shouldBe Some("cassandra")
//        ////          plan1.operations.write.outputSource shouldBe s"cassandra:$keyspace:$table"
//        //
//        ////          plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
//        //          plan2.operations.reads.get.head.extra.get("sourceType") shouldBe Some("cassandra")
//        //        }
//
//        for {
//          (plan2, _) <- captor.lineageOf {
//            testData
//              .writeTo(s"cass100.$keyspace.$table")
//              .append()
//
//            val df = spark
//              .read
//              .table(s"cass100.$keyspace.$table")
//
//            df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
//          }
//        } yield {
//          println(plan2 )
//          Succeeded
//        }
//
//      }
//    }
//
//}
