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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.io.FileUtils.readFileToString
import org.apache.spark.SPARK_VERSION
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.json.DefaultJacksonJsonSerDe
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.commons.version.Version._
import za.co.absa.spline.harvester.SparkLineageInitializer.SparkSessionWrapper
import za.co.absa.spline.test.fixture.SparkFixture

import java.io.File

class HadoopFsLineageDispatcherSpec
  extends AnyFlatSpec
    with Matchers
    with SparkFixture
    with DefaultJacksonJsonSerDe {

  behavior of "HadoopFsLineageDispatcher"

  it should "save lineage file to a filesystem" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    withCustomSparkSession(_
      .config("spark.spline.lineageDispatcher", "hadoopFs")
      .config("spark.spline.lineageDispatcher.hadoopFs.className", classOf[HadoopFsLineageDispatcher].getName)) {
      spark =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", ".parquet", pathOnly = true).deleteOnExit().path

        spark.enableLineageTracking()
        dummyDF.write.save(destPath.toString)

        val lineageFile = new File(destPath.toFile, "_LINEAGE")
        lineageFile.exists should be(true)
        lineageFile.length should be > 0L

        val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
        lineageJson should contain key "executionPlan"
        lineageJson should contain key "executionEvent"
        lineageJson("executionPlan")("id") should equal(lineageJson("executionEvent")("planId"))
    }
  }
}
