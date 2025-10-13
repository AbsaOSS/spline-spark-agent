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
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.commons.version.Version._
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture

import java.io.File

class HDFSLineageDispatcherSpec
  extends AsyncFlatSpec
    with Matchers
    with SparkFixture
    with SplineFixture {

  behavior of "HDFSLineageDispatcher"

  val lineageDispatcherConfigKeyName = "spark.spline.lineageDispatcher"
  val lineageDispatcherConfigValueName = "hdfs"
  val lineageDispatcherConfigClassNameKeyName = s"$lineageDispatcherConfigKeyName.$lineageDispatcherConfigValueName.className"
  val lineageDispatcherConfigCustomLineagePathKeyName = s"$lineageDispatcherConfigKeyName.$lineageDispatcherConfigValueName.customLineagePath"
  val destFilePathExtension = ".parquet"

  it should "save lineage file to a filesystem in DEFAULT mode" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    withIsolatedSparkSession(_
      .config(lineageDispatcherConfigKeyName, lineageDispatcherConfigValueName)
      .config(lineageDispatcherConfigClassNameKeyName, classOf[HDFSLineageDispatcher].getName)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", destFilePathExtension, pathOnly = true).deleteOnExit()

        for {
          (_, _) <- captor.lineageOf(dummyDF.write.save(destPath.asString))
        } yield {
          val lineageFile = new File(destPath.asString, "_LINEAGE")
          lineageFile.exists should be(true)
          lineageFile.length should be > 0L

          val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
          lineageJson should contain key "executionPlan"
          lineageJson should contain key "executionEvent"
          lineageJson("executionPlan")("id") should equal(lineageJson("executionEvent")("planId"))
        }
      }
    }
  }

  Seq(
    ("without customLineagePath config", None),
    ("with empty string customLineagePath", Some("")),
    ("with whitespace-only customLineagePath", Some("   "))
  ).foreach { case (scenarioDesc, customPathValue) =>
    it should s"use DEFAULT mode $scenarioDesc" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
      val builder = (b: SparkSession.Builder) => {
        val configured = b
          .config(lineageDispatcherConfigKeyName, lineageDispatcherConfigValueName)
          .config(lineageDispatcherConfigClassNameKeyName, classOf[HDFSLineageDispatcher].getName)
        customPathValue.fold(configured)(path => configured.config(lineageDispatcherConfigCustomLineagePathKeyName, path))
      }
      
      withIsolatedSparkSession(builder) { implicit spark =>
        withLineageTracking { captor =>
          import spark.implicits._
          val dummyDF = Seq((1, 2)).toDF
          val destPath = TempDirectory("spline_", destFilePathExtension, pathOnly = true).deleteOnExit()

          for {
            (_, _) <- captor.lineageOf(dummyDF.write.save(destPath.asString))
          } yield {
            val lineageFile = new File(destPath.asString, "_LINEAGE")
            lineageFile.exists should be(true)
            lineageFile.length should be > 0L

            val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
            lineageJson should contain key "executionPlan"
            lineageJson should contain key "executionEvent"
            lineageJson("executionPlan")("id") should equal(lineageJson("executionEvent")("planId"))
          }
        }
      }
    }
  }

  it should "save lineage files in a custom lineage path" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    val centralizedPath = TempDirectory("spline_centralized").deleteOnExit()

    withIsolatedSparkSession(_
      .config(lineageDispatcherConfigKeyName, lineageDispatcherConfigValueName)
      .config(lineageDispatcherConfigClassNameKeyName, classOf[HDFSLineageDispatcher].getName)
      .config(lineageDispatcherConfigCustomLineagePathKeyName, centralizedPath.asString)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        
        // Test with multiple data writes to verify unique filenames
        val dummyDF1 = Seq((1, 2)).toDF
        val dummyDF2 = Seq((3, 4)).toDF
        val destPath1 = TempDirectory("spline_1_", destFilePathExtension, pathOnly = true).deleteOnExit()
        val destPath2 = TempDirectory("spline_2_", destFilePathExtension, pathOnly = true).deleteOnExit()

        for {
          (_, _) <- captor.lineageOf(dummyDF1.write.save(destPath1.asString))
          (_, _) <- captor.lineageOf(dummyDF2.write.save(destPath2.asString))
        } yield {
          val centralizedDir = new File(centralizedPath.asString)
          centralizedDir.exists should be(true)
          centralizedDir.isDirectory should be(true)
          
          val appId = spark.sparkContext.applicationId
          val appName = spark.sparkContext.appName
          val appNameCleaned = appName.replaceAll(r"[^a-zA-Z0-9_-]".r, "_")

          // Should have 2 lineage files (one for each write operation)
          val lineageFiles = Option(centralizedDir.listFiles()).getOrElse(Array.empty[File])
          val lineageFilesOnly = lineageFiles.filter(f => f.isFile && !f.getName.endsWith(".crc"))
          lineageFilesOnly.length should be(2)

          // Verify naming convention aligns with centralized lineage pattern (timestamp_appName_appId)
          val filenamePattern = """\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}-\\d{3}_.+_.+""".r
          lineageFilesOnly.foreach { file =>
            val name = file.getName
            withClue(s"Lineage filename '$name' should follow the timestamp_appName_appId pattern") {
              filenamePattern.matches(name) shouldBe true
            }
            name should include (appId) and include (appNameCleaned)
          }

          // Verify each file has the correct format and content
          lineageFilesOnly.map { lineageFile =>
            val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
            lineageJson should contain key "executionPlan"
            lineageJson should contain key "executionEvent"
          }

          // Verify no lineage files in destination directories
          val lineageFileInDest1 = new File(destPath1.asString, "_LINEAGE")
          val lineageFileInDest2 = new File(destPath2.asString, "_LINEAGE")
          lineageFileInDest1.exists should be(false)
          lineageFileInDest2.exists should be(false)
        }
      }
    }
  }

}
