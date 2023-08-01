/*
 * Copyright 2022 ABSA Group Limited
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

import org.scalatest.OneInstancePerTest
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.commons.io.TempFile
import za.co.absa.spline.test.LineageWalker
import za.co.absa.spline.test.ProducerModelImplicits._
import za.co.absa.spline.test.SplineMatchers._
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}

class ViewAttributeLineageSpec
  extends AsyncFlatSpec
    with OneInstancePerTest
    with Matchers
    with SparkFixture
    with SparkDatabaseFixture
    with SplineFixture {

  it should "trace attribute dependencies through persistent view" in
    withIsolatedSparkSession(_.enableHiveSupport()) { implicit spark =>
      withLineageTracking { captor =>
        val databaseName = s"unitTestDatabase_${this.getClass.getSimpleName}"
        withDatabase(databaseName,
          ("path", "(x STRING) USING HIVE", Seq("Monika", "Buba"))
        ) {

          withNewSparkSession { innerSpark =>
            innerSpark.sql(s"DROP VIEW IF EXISTS $databaseName.test_source_vw")
            innerSpark.sql(s"CREATE VIEW $databaseName.test_source_vw AS SELECT * FROM $databaseName.path")
          }

          for {
            (plan, _) <- captor.lineageOf {
              spark.sql("SELECT * FROM test_source_vw")
                .write
                .format("hive")
                .mode("overwrite")
                .saveAsTable("view_test_target")
            }
          } yield {
            implicit val walker: LineageWalker = LineageWalker(plan)

            val writeOutput = plan.operations.write.childOperation.outputAttributes
            val outAttribute = writeOutput(0)

            val reads = plan.operations.reads
            val readOutput = reads(0).outputAttributes
            val inAttribute = readOutput(0)

            outAttribute should dependOn(inAttribute)
          }
        }
      }
    }

  it should "trace attribute dependencies through global temp view" in
    withIsolatedSparkSession(_.enableHiveSupport()) { implicit spark =>
      withLineageTracking { captor =>
        val databaseName = s"unitTestDatabase_${this.getClass.getSimpleName}"
        withDatabase(databaseName,
          ("path", "(x String) USING HIVE", Seq("Monika", "Buba"))
        ) {
          for {
            (plan, _) <- captor.lineageOf {
              spark.sql("SELECT * FROM path")
                .createOrReplaceGlobalTempView("my_global_temp_view")

              spark.sql("SELECT * FROM global_temp.my_global_temp_view")
                .write
                .mode("overwrite")
                .saveAsTable("view_test_target")
            }
          } yield {
            implicit val walker: LineageWalker = LineageWalker(plan)

            val writeOutput = plan.operations.write.childOperation.outputAttributes
            val outAttribute = writeOutput(0)

            val reads = plan.operations.reads
            val readOutput = reads(0).outputAttributes
            val inAttribute = readOutput(0)

            outAttribute should dependOn(inAttribute)
          }
        }
      }
    }

  it should "trace attribute dependencies through local temp view" in
    withIsolatedSparkSession(_.enableHiveSupport()) { implicit spark =>
      withLineageTracking { captor =>
        val databaseName = s"unitTestDatabase_${this.getClass.getSimpleName}"
        withDatabase(databaseName,
          ("path", "(x STRING) USING HIVE", Seq("Monika", "Buba"))
        ) {
          for {
            (plan, _) <- captor.lineageOf {
              spark.sql("SELECT * FROM path")
                .createOrReplaceTempView("my_local_temp_view")

              spark.sql("SELECT * FROM my_local_temp_view")
                .write
                .mode("overwrite")
                .saveAsTable("view_test_target")
            }
          } yield {
            implicit val walker: LineageWalker = LineageWalker(plan)

            val writeOutput = plan.operations.write.childOperation.outputAttributes
            val outAttribute = writeOutput(0)

            val reads = plan.operations.reads
            val readOutput = reads(0).outputAttributes
            val inAttribute = readOutput(0)

            outAttribute should dependOn(inAttribute)
          }
        }
      }
    }

  it should "handle read as a view child" in
    withIsolatedSparkSession(_.enableHiveSupport()) { implicit spark =>
      withLineageTracking { captor =>
        for {
          (plan, _) <- captor.lineageOf {
            spark.read
              .csv("data/cities.csv")
              .createOrReplaceTempView("my_local_temp_view")

            spark.sql("SELECT * FROM my_local_temp_view")
              .write
              .csv(TempFile(pathOnly = true).deleteOnExit().asString)
          }
        } yield {
          implicit val walker: LineageWalker = LineageWalker(plan)

          val writeOutput = plan.operations.write.childOperation.outputAttributes
          val outAttribute = writeOutput(0)

          val reads = plan.operations.reads
          val readOutput = reads(0).outputAttributes
          val inAttribute = readOutput(0)

          outAttribute should dependOn(inAttribute)
        }

      }
    }

}
