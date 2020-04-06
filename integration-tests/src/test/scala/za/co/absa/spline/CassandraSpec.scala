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
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture
import com.datastax.driver.core.Cluster
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

class CassandraSpec
  extends AnyFlatSpec
    with Matchers
    with SparkFixture
    with SplineFixture {

  it should "support Cassandra as a write source" in {
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {

        spark.conf.set("spark.cassandra.connection.port", "9142") //non default port for embedded cassandra

        val keyspace = "test_keyspace"
        val table = "test_table"

        //Embedded Cassandra setup
        EmbeddedCassandraServerHelper.startEmbeddedCassandra()
        val cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build()
        val session = cluster.connect()
        session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace  WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
        session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (ID INT, NAME TEXT, PRIMARY KEY (ID))")

        val testData: DataFrame = {
          val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }

        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write
          .mode(SaveMode.Overwrite)
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> table, "keyspace" -> keyspace, "confirm.truncate" -> "true"))
          .save())

        val (plan2, _) = lineageCaptor.lineageOf {
          val df = spark
            .read
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> table, "keyspace" -> keyspace))
            .load()

          df.write
            .mode(SaveMode.Append)
            .format("org.apache.spark.sql.cassandra")
            .options(Map("table" -> table, "keyspace" -> keyspace))
            .save()
        }

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType") shouldBe Some("cassandra")
        plan1.operations.write.outputSource shouldBe s"cassandra:$keyspace:$table"

        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType") shouldBe Some("cassandra")
        plan2.operations.write.append shouldBe true

      })
    })
  }
}
