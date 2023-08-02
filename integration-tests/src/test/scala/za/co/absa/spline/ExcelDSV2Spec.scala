/*
 * Copyright 2023 ABSA Group Limited
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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.commons.version.Version.VersionStringInterpolator
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{ReleasableResourceFixture, SparkFixture}

class ExcelDSV2Spec
  extends AsyncFlatSpec
    with BeforeAndAfterAll
    with Matchers
    with SparkFixture
    with SplineFixture
    with ReleasableResourceFixture {


  it should "support Excel files as a source V2" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        val outputPath = TempDirectory(pathOnly = true).deleteOnExit().asString

        for {
          (plan1, _) <- captor.lineageOf {
            val dataFrame = spark
              .read
              .format("excel")
              .option("header", "true")
              .load("data/test.xlsx")

            dataFrame
              .write
              .format("excel")
              .option("header", "true")
              .mode("overwrite")
              .save(outputPath)
          }
        } yield {
          plan1.operations.write.append shouldBe false
          plan1.operations.write.extra("destinationType") shouldBe Some("excel")
          plan1.operations.write.outputSource shouldBe s"file:$outputPath"

          plan1.operations.reads.head.inputSources.head should endWith("data/test.xlsx")
          plan1.operations.reads.head.extra("sourceType") shouldBe Some("excel")
        }
      }
    }

}
