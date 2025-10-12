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

  it should "save lineage file to a filesystem in DEFAULT mode (no customLineagePath)" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    withIsolatedSparkSession(_
      .config("spark.spline.lineageDispatcher", "hdfs")
      .config("spark.spline.lineageDispatcher.hdfs.className", classOf[HDFSLineageDispatcher].getName)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", ".parquet", pathOnly = true).deleteOnExit()

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

  it should "save lineage file in CENTRALIZED mode with customLineagePath" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    val centralizedPath = TempDirectory("spline_centralized_", "", pathOnly = true).deleteOnExit()

    withIsolatedSparkSession(_
      .config("spark.spline.lineageDispatcher", "hdfs")
      .config("spark.spline.lineageDispatcher.hdfs.className", classOf[HDFSLineageDispatcher].getName)
      .config("spark.spline.lineageDispatcher.hdfs.customLineagePath", centralizedPath.asString)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", ".parquet", pathOnly = true).deleteOnExit()

        for {
          (_, _) <- captor.lineageOf(dummyDF.write.save(destPath.asString))
        } yield {
          // Lineage should NOT be in the destination directory
          val lineageFileInDest = new File(destPath.asString, "_LINEAGE")
          lineageFileInDest.exists should be(false)

          // Lineage should be in centralized directory with timestamp-based filename
          val centralizedDir = new File(centralizedPath.asString)
          val lineageFiles = centralizedDir.listFiles()
          lineageFiles should not be null
          lineageFiles.length should be(1)

          val lineageFile = lineageFiles(0)
          lineageFile.length should be > 0L

          // Verify filename format: {timestamp}_{fileName}_{appId}
          val filename = lineageFile.getName
          // Should match pattern: yyyy-MM-dd_HH-mm-ss-SSS__LINEAGE_app-...
          filename should include("_LINEAGE_")
          filename should startWith regex """\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}-\d{3}"""

          val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
          lineageJson should contain key "executionPlan"
          lineageJson should contain key "executionEvent"
          lineageJson("executionPlan")("id") should equal(lineageJson("executionEvent")("planId"))
        }
      }
    }
  }

  it should "use DEFAULT mode when customLineagePath is empty string" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    withIsolatedSparkSession(_
      .config("spark.spline.lineageDispatcher", "hdfs")
      .config("spark.spline.lineageDispatcher.hdfs.className", classOf[HDFSLineageDispatcher].getName)
      .config("spark.spline.lineageDispatcher.hdfs.customLineagePath", "") // Empty string
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", ".parquet", pathOnly = true).deleteOnExit()

        for {
          (_, _) <- captor.lineageOf(dummyDF.write.save(destPath.asString))
        } yield {
          // Empty string should trigger DEFAULT mode, lineage should be in destination directory
          val lineageFile = new File(destPath.asString, "_LINEAGE")
          lineageFile.exists should be(true)
          lineageFile.length should be > 0L

          val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
          lineageJson should contain key "executionPlan"
          lineageJson should contain key "executionEvent"
        }
      }
    }
  }

  it should "use DEFAULT mode when customLineagePath is whitespace only" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    withIsolatedSparkSession(_
      .config("spark.spline.lineageDispatcher", "hdfs")
      .config("spark.spline.lineageDispatcher.hdfs.className", classOf[HDFSLineageDispatcher].getName)
      .config("spark.spline.lineageDispatcher.hdfs.customLineagePath", "   ") // Whitespace only
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", ".parquet", pathOnly = true).deleteOnExit()

        for {
          (_, _) <- captor.lineageOf(dummyDF.write.save(destPath.asString))
        } yield {
          // Whitespace should trigger DEFAULT mode, lineage should be in destination directory
          val lineageFile = new File(destPath.asString, "_LINEAGE")
          lineageFile.exists should be(true)
          lineageFile.length should be > 0L

          val lineageJson = readFileToString(lineageFile, "UTF-8").fromJson[Map[String, Map[String, _]]]
          lineageJson should contain key "executionPlan"
          lineageJson should contain key "executionEvent"
        }
      }
    }
  }

  it should "generate chronologically sortable filenames in CENTRALIZED mode" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    val centralizedPath = TempDirectory("spline_centralized_", "", pathOnly = true).deleteOnExit()

    withIsolatedSparkSession(_
      .config("spark.spline.lineageDispatcher", "hdfs")
      .config("spark.spline.lineageDispatcher.hdfs.className", classOf[HDFSLineageDispatcher].getName)
      .config("spark.spline.lineageDispatcher.hdfs.customLineagePath", centralizedPath.asString)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        // Write multiple lineage files
        val futures = for (i <- 1 to 3) yield {
          val dummyDF = Seq((i, i * 2)).toDF
          val destPath = TempDirectory(s"spline_$i", ".parquet", pathOnly = true).deleteOnExit()
          captor.lineageOf(dummyDF.write.save(destPath.asString))
        }

        for {
          _ <- futures.head
          _ <- futures(1)
          _ <- futures(2)
        } yield {
          val centralizedDir = new File(centralizedPath.asString)
          val lineageFiles = centralizedDir.listFiles().sorted
          lineageFiles.length should be(3)

          // Verify filenames are in chronological order (lexicographic = chronological due to timestamp-first format)
          val filenames = lineageFiles.map(_.getName).toList
          filenames shouldBe filenames.sorted

          // Verify all filenames start with date prefix (yyyy-MM-dd)
          filenames.foreach { name =>
            name should fullyMatch regex """\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}-\d{3}_.+"""
          }
        }
      }
    }
  }

  it should "create nested directories in CENTRALIZED mode" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    val centralizedBasePath = TempDirectory("spline_base_", "", pathOnly = true).deleteOnExit()
    val nestedPath = new File(centralizedBasePath.asString, "level1/level2/level3").getAbsolutePath

    withIsolatedSparkSession(_
      .config("spark.spline.lineageDispatcher", "hdfs")
      .config("spark.spline.lineageDispatcher.hdfs.className", classOf[HDFSLineageDispatcher].getName)
      .config("spark.spline.lineageDispatcher.hdfs.customLineagePath", nestedPath)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dummyDF = Seq((1, 2)).toDF
        val destPath = TempDirectory("spline_", ".parquet", pathOnly = true).deleteOnExit()

        for {
          (_, _) <- captor.lineageOf(dummyDF.write.save(destPath.asString))
        } yield {
          // Verify nested directories were created
          val nestedDir = new File(nestedPath)
          nestedDir.exists should be(true)
          nestedDir.isDirectory should be(true)

          // Verify lineage file was written
          val lineageFiles = nestedDir.listFiles()
          lineageFiles should not be null
          lineageFiles.length should be(1)
          lineageFiles(0).length should be > 0L
        }
      }
    }
  }

}
