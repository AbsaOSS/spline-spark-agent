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

import com.mongodb.MongoClient
import de.flapdoodle.embed.mongo.config.Net
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import de.flapdoodle.embed.mongo.MongodStarter

class MongoDBSpec
  extends AnyFlatSpec
    with Matchers
    with SparkFixture
    with SplineFixture {

  it should "support MongoDB as a write source" in {
    val starter = MongodStarter.getDefaultInstance


    val bindIp = "localhost"
    val port = 12345
    val databaseName = "test"
    val collection = "testCollection"
    val mongodConfig = new MongodConfigBuilder().version(Version.Main.PRODUCTION).net(new Net(bindIp, port, Network.localhostIsIPv6)).build()

    val mongodExecutable = starter.prepare(mongodConfig)
    val mongod = mongodExecutable.start()
    val mongo = new MongoClient(bindIp, port)

    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {

        val testData: DataFrame = {
          val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }
        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write
          .format("com.mongodb.spark.sql.DefaultSource")
          .option("uri", "mongodb://127.0.0.1:12345")
          .option("database", databaseName)
          .option("collection", collection)
          .save()
        )

        val (plan2, _) = lineageCaptor.lineageOf {
          val df = spark
            .read
            .format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", "mongodb://127.0.0.1:12345")
            .option("database", databaseName)
            .option("collection", collection)
            .option("partitioner", "MongoSplitVectorPartitioner")
            .load()

          df
            .write
            .mode(SaveMode.Overwrite)
            .format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", "mongodb://127.0.0.1:12345")
            .option("database", databaseName)
            .option("collection", collection)
            .save()
        }

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType") shouldBe Some("mongodb")
        plan1.operations.write.outputSource shouldBe s"mongodb://127.0.0.1:12345/$databaseName.$collection"

        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType") shouldBe Some("mongodb")
        plan2.operations.write.append shouldBe false

      })
    })
  }
}
