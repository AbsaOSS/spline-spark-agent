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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{ReleasableResourceFixture, SparkFixture}

class MongoDBSpec
  extends AsyncFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with SparkFixture
    with SplineFixture
    with ReleasableResourceFixture {

  val databaseName = "test"
  val collection = "testCollection"
  val sparkFormat = "com.mongodb.spark.sql.DefaultSource"

  it should "support MongoDB as a write source" in {
    usingResource(new MongoDBContainer("mongo:4.0.10")) { container =>
      container.start()
      Wait.forHealthcheck

      val host = container.getHost
      val port = container.getFirstMappedPort

      withNewSparkSession { implicit spark =>
        withLineageTracking { captor =>

          val testData: DataFrame = {
            val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
            val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
            spark.sqlContext.createDataFrame(rdd, schema)
          }

          for {
            (plan1, _) <- captor.lineageOf(testData
              .write
              .format(sparkFormat)
              .option("uri", s"mongodb://$host:$port")
              .option("database", databaseName)
              .option("collection", collection)
              .save()
            )

            (plan2, _) <- captor.lineageOf {
              val df = spark
                .read
                .format(sparkFormat)
                .option("uri", s"mongodb://$host:$port")
                .option("database", databaseName)
                .option("collection", collection)
                .option("partitioner", "MongoSplitVectorPartitioner")
                .load()

              df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
            }
          } yield {
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("mongodb")
            plan1.operations.write.outputSource shouldBe s"mongodb://$host:$port/$databaseName.$collection"

            plan2.operations.reads.head.inputSources.head shouldBe plan1.operations.write.outputSource
            plan2.operations.reads.head.extra("sourceType") shouldBe Some("mongodb")
            plan2.operations.write.append shouldBe false
          }
        }
      }
    }
  }

}
