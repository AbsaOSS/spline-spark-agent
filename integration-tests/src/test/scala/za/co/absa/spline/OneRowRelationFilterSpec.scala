/*
 * Copyright 2021 ABSA Group Limited
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

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempFile
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}
import za.co.absa.spline.test.harvester.dispatcher.NoOpLineageDispatcher

class OneRowRelationFilterSpec extends AsyncFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture
  with SparkDatabaseFixture {

  it should "produce lineage without OneRowRelation operation" in
    withRestartingSparkContext {
      withCustomSparkSession(_
        .config("spark.spline.lineageDispatcher", "noOp")
        .config("spark.spline.lineageDispatcher.noOp.className", classOf[NoOpLineageDispatcher].getName)
      ) { implicit spark =>
        withRealConfigLineageTracking { lineageCaptor =>
          withDatabase("testDB") {
            for {
              (plan, _) <- lineageCaptor.lineageOf {
                spark
                  .sql("SELECT 'Green' AS data_quality_status, 'Batch Started' AS batch_status")
                  .write
                  .saveAsTable("t1")
              }
            } yield {
              val Seq(op) = plan.operations.other.get

              op.name.get should be("Project")
              op.childIds should be(None)
              op.output.get.size should be(2)
            }
          }
        }
      }
    }

  it should "handle operations without children" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { lineageCaptor =>
        for {
          (plan, _) <- lineageCaptor.lineageOf {
            spark
              .sql("with sub as (SELECT 1.0 AS a) select a FROM sub")
              .write.csv(TempFile(pathOnly = true).deleteOnExit().asString)
          }
        } yield {
          plan.operations.other.get.exists(_.name.get.startsWith("OneRowRelation")) should be(false)
        }
      }
    }

}
