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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.CassandraContainer
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.commons.version.Version.VersionStringInterpolator
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{ReleasableResourceFixture, SparkFixture}

import scala.util.Using


class CassandraSpec
  extends AsyncFlatSpec
    with Matchers
    with SparkFixture
    with SplineFixture
    with ReleasableResourceFixture {

  it should "support Cassandra on older Spark versions" taggedAs ignoreIf(ver"$SPARK_VERSION" >= ver"3.0.0") in {
    usingResource(new CassandraContainer("cassandra:3.11.3")) { container =>
      container.start()

      withNewSparkSession { implicit spark =>
        withLineageTracking { lineageCaptor =>
          spark.conf.set("spark.cassandra.connection.host", container.getHost)
          spark.conf.set("spark.cassandra.connection.port", container.getFirstMappedPort.toString)

          val keyspace = "test_keyspace"
          val table = "test_table"

          Using.resource(container.getCluster.connect()) { session =>
            session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace  WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
            session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (id INT, name TEXT, PRIMARY KEY (id))")
          }

          val testData: DataFrame = {
            val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
            val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
            spark.sqlContext.createDataFrame(rdd, schema)
          }

          for {
            (plan1, _) <- lineageCaptor.lineageOf(testData
              .write
              .mode(SaveMode.Overwrite)
              .format("org.apache.spark.sql.cassandra")
              .options(Map("table" -> table, "keyspace" -> keyspace, "confirm.truncate" -> "true"))
              .save())

            (plan2, _) <- lineageCaptor.lineageOf {
              val df = spark
                .read
                .format("org.apache.spark.sql.cassandra")
                .options(Map("table" -> table, "keyspace" -> keyspace))
                .load()

              df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
            }
          } yield {
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("cassandra")
            plan1.operations.write.outputSource shouldBe s"cassandra:$keyspace:$table"

            plan2.operations.reads.head.inputSources.head shouldBe plan1.operations.write.outputSource
            plan2.operations.reads.head.extra("sourceType") shouldBe Some("cassandra")
          }
        }
      }
    }
  }

  it should "support Cassandra on Spark 3.0 and higher" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in {
    usingResource(new CassandraContainer("cassandra:3.11.2")) { container =>
      container.start()

      withNewSparkSession { implicit spark =>
        withLineageTracking { lineageCaptor =>
          spark.conf.set("spark.sql.catalog.casscat", "com.datastax.spark.connector.datasource.CassandraCatalog")
          spark.conf.set("spark.sql.catalog.casscat.spark.cassandra.connection.host", container.getHost)
          spark.conf.set("spark.sql.catalog.casscat.spark.cassandra.connection.port", container.getFirstMappedPort.toString)

          val keyspace = "test_keyspace"
          val table = "test_table"

          Using.resource(container.getCluster.connect()) { session =>
            session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace  WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
            session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (id INT, name TEXT, PRIMARY KEY (id))")
          }

          val testData: DataFrame = {
            val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
            val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
            spark.sqlContext.createDataFrame(rdd, schema)
          }

          for {
            (plan1, _) <- lineageCaptor.lineageOf {
              testData.createOrReplaceTempView("tdview")
              spark.sql("""INSERT INTO casscat.test_keyspace.test_table TABLE tdview;""")
            }

            (plan2, _) <- lineageCaptor.lineageOf {
              val df = spark.sql("SELECT * FROM casscat.test_keyspace.test_table")
              df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
            }
          } yield {
            plan1.operations.write.append shouldBe true
            plan1.operations.write.extra("destinationType") shouldBe Some("cassandra")
            plan1.operations.write.outputSource shouldBe s"cassandra:$keyspace:$table"

            plan2.operations.reads.head.inputSources.head shouldBe plan1.operations.write.outputSource
            plan2.operations.reads.head.extra("sourceType") shouldBe Some("cassandra")
          }
        }
      }
    }
  }

}
