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

import org.apache.spark.sql.{Row, RowFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{ReleasableResourceFixture, SparkFixture}

import java.util

class SnowflakeSpec
  extends AsyncFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with SparkFixture
    with SplineFixture
    with ReleasableResourceFixture {

  val tableName = "testTable"
  val schemaName = "testSchema"
  val warehouseName = "testWarehouse"
  val databaseName = "test"
  val sparkFormat = "net.snowflake.spark.snowflake"

  it should "support snowflake as a read and write source" in {
    usingResource(new GenericContainer(DockerImageName.parse("localstack/snowflake"))) { container =>
      container.start()
      Wait.forHealthcheck

      val host = container.getHost

      withNewSparkSession { implicit spark =>

        withLineageTracking { captor =>
          val sfOptions = Map(
            "sfURL" -> "snowflake.localhost.localstack.cloud",
            "sfUser" -> "test",
            "sfPassword" -> "test",
            "sfDatabase" -> databaseName,
            "sfWarehouse" -> warehouseName,
            "sfSchema" -> schemaName
          )

          // Define your data as a Java List
          val data = new util.ArrayList[Row]()
          data.add(RowFactory.create(1.asInstanceOf[Object]))
          data.add(RowFactory.create(2.asInstanceOf[Object]))
          data.add(RowFactory.create(3.asInstanceOf[Object]))

          // Use the method to create DataFrame
          val testData = spark.sqlContext.createDataFrame(data, classOf[Row])

          for {
            (writePlan, _) <- captor.lineageOf(
              testData.write
              .format(sparkFormat)
              .options(sfOptions)
              .option("dbtable", tableName)
              .mode("overwrite")
              .save()
            )

            (readPlan, _) <- captor.lineageOf {
              val df = spark.read.format(sparkFormat)
                .options(sfOptions)
                .option("dbtable", tableName)  // specify the source table
                .load()

              df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
            }
          } yield {
            writePlan.operations.write.append shouldBe false
            writePlan.operations.write.extra("destinationType") shouldBe Some("snowflake")
            writePlan.operations.write.outputSource shouldBe s"snowflake://$host.$warehouseName.$databaseName.$schemaName.$tableName"

            readPlan.operations.reads.head.inputSources.head shouldBe writePlan.operations.write.outputSource
            readPlan.operations.reads.head.extra("sourceType") shouldBe Some("snowflake")
            readPlan.operations.write.append shouldBe false
          }
        }
      }
    }
  }

}
