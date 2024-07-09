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

package za.co.absa.spline.harvester

import org.apache.commons.io.FileUtils
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SaveMode
import org.scalatest.Inside._
import org.scalatest.OneInstancePerTest
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.spline.commons.io.{TempDirectory, TempFile}
import za.co.absa.spline.commons.version.Version._
import za.co.absa.spline.model.dt
import za.co.absa.spline.producer.model._
import za.co.absa.spline.test.LineageWalker
import za.co.absa.spline.test.ProducerModelImplicits._
import za.co.absa.spline.test.SplineMatchers.dependOn
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}

import java.util.UUID
import scala.concurrent.Future
import scala.language.reflectiveCalls
import scala.util.Try

class LineageHarvesterSpec extends AsyncFlatSpec
  with OneInstancePerTest
  with Matchers
  with SparkFixture
  with SplineFixture
  with SparkDatabaseFixture {

  import za.co.absa.spline.harvester.LineageHarvesterSpec._

  it should "produce deterministic output" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = TempDirectory(pathOnly = true).deleteOnExit().asString

        def testJob(): Unit = spark
          .createDataset(Seq(TestRow(1, 2.3, "text")))
          .filter('i > 0)
          .sort('s)
          .select('d + 42 as "x")
          .write
          .mode(SaveMode.Append)
          .save(testDest)

        for {
          (plan1, _) <- captor.lineageOf(testJob())
          (plan2, _) <- captor.lineageOf(testJob())
        } yield {
          plan1 shouldEqual plan2
        }
      }
    }

  it should "support empty data frame" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = TempDirectory(pathOnly = true).deleteOnExit().asString

        for {
          (plan, _) <- captor.lineageOf(spark.emptyDataset[TestRow].write.save(testDest))
        } yield {
          inside(plan) {
            case ExecutionPlan(_, _, _, _, Operations(_, Seq(), Seq(op)), _, _, _, _, _) =>
              op.id should be("op-1")
              op.name should be("LocalRelation")
              op.childIds should be(Seq.empty)
              op.output should not be Seq.empty
              op.extra should be(Map.empty)
          }
        }
      }
    }

  it should "support simple non-empty data frame" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpLocal = TempDirectory(pathOnly = true).deleteOnExit()
        val tmpPath = tmpLocal.asString

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))

        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          val write = plan.operations.write
          write.id shouldBe "op-0"
          write.name should (be("InsertIntoHadoopFsRelationCommand") or be("SaveIntoDataSourceCommand"))
          write.childIds should be(Seq("op-1"))
          write.outputSource should be(tmpLocal.toURI.toString.stripSuffix("/"))
          write.params should be(Map("path" -> tmpPath))
          write.extra should be(Map("destinationType" -> Some("parquet")))

          plan.operations.reads should be(Seq.empty)

          plan.operations.other.size should be(1)

          val op = plan.operations.other.head
          op.id should be("op-1")
          op.name should be("LocalRelation")
          op.childIds.size should be(0)
          op.output should be(Seq("attr-0", "attr-1", "attr-2"))

          plan.attributes should be(Seq(
            Attribute("attr-0", Some(integerType.id), Seq.empty, Map.empty, "i"),
            Attribute("attr-1", Some(doubleType.id), Seq.empty, Map.empty, "d"),
            Attribute("attr-2", Some(stringType.id), Seq.empty, Map.empty, "s")
          ))
        }
      }
    }

  it should "support filter and column renaming operations" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpLocal = TempDirectory(pathOnly = true).deleteOnExit()
        val tmpPath = tmpLocal.asString

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
          .withColumnRenamed("i", "A")
          .filter($"A".notEqual(5))

        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          val write = plan.operations.write
          write.id shouldBe "op-0"

          write.name should (be("InsertIntoHadoopFsRelationCommand") or be("SaveIntoDataSourceCommand"))
          write.childIds should be(Seq("op-1"))
          write.outputSource should be(tmpLocal.toURI.toString.stripSuffix("/"))
          write.append should be(false)
          write.params should be(Map("path" -> tmpPath))
          write.extra should be(Map("destinationType" -> Some("parquet")))

          plan.operations.reads should be(Seq.empty)

          plan.operations.other.size should be(3)

          val localRelation = plan.operations.other.head
          localRelation.id should be("op-3")
          localRelation.name should be("LocalRelation")
          localRelation.childIds.size should be(0)
          localRelation.output should be(Seq("attr-0", "attr-1", "attr-2"))

          val project = plan.operations.other(1)
          project.id should be("op-2")
          project.name should be("Project")
          project.childIds should be(Seq("op-3"))
          project.output should be(Seq("attr-3", "attr-1", "attr-2"))
          project.params("projectList") should be(Some(Seq(
            ExprRef("expr-0"),
            AttrRef("attr-1"),
            AttrRef("attr-2")
          )))

          val filter = plan.operations.other(2)
          filter.id should be("op-1")
          filter.name should be("Filter")
          filter.childIds should be(Seq("op-2"))
          filter.output should be(Seq("attr-3", "attr-1", "attr-2"))
          filter.params("condition") should be(Some(ExprRef("expr-1")))

          plan.attributes should be(Seq(
            Attribute("attr-0", Some(integerType.id), Seq.empty, Map.empty, "i"),
            Attribute("attr-1", Some(doubleType.id), Seq.empty, Map.empty, "d"),
            Attribute("attr-2", Some(stringType.id), Seq.empty, Map.empty, "s"),
            Attribute("attr-3", Some(integerType.id), Seq(ExprRef("expr-0")), Map.empty, "A")
          ))
        }
      }
    }

  it should "support union operation, forming a diamond graph" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpLocal = TempDirectory(pathOnly = true).deleteOnExit()
        val tmpPath = tmpLocal.asString

        val initialDF = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        val filteredDF1 = initialDF.filter($"i".notEqual(5))
        val filteredDF2 = initialDF.filter($"d".notEqual(5.0))
        val df = filteredDF1.union(filteredDF2)

        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          implicit val walker: LineageWalker = LineageWalker(plan)

          val write = plan.operations.write
          write.name should (be("InsertIntoHadoopFsRelationCommand") or be("SaveIntoDataSourceCommand"))
          write.outputSource should be(tmpLocal.toURI.toString.stripSuffix("/"))
          write.append should be(false)
          write.params should be(Map("path" -> tmpPath))
          write.extra should be(Map("destinationType" -> Some("parquet")))

          val union = write.childOperation
          union.name should be("Union")
          union.childIds.size should be(2)

          val Seq(filter1, maybeFilter2) = union.childOperations
          filter1.name should be("Filter")
          filter1.params should contain key "condition"

          val filter2 =
            if (maybeFilter2.name == "Project") {
              maybeFilter2.childOperation // skip additional select (in Spark 3.0 and 3.1 only)
            } else {
              maybeFilter2
            }

          filter2.name should be("Filter")
          filter2.params should contain key "condition"

          val localRelation1 = filter1.childOperation
          localRelation1.name should be("LocalRelation")

          val localRelation2 = filter2.childOperation
          localRelation2.name should be("LocalRelation")

          inside(union.outputAttributes) { case Seq(i, d, s) =>
            inside(filter1.outputAttributes) { case Seq(f1I, f1D, f1S) =>
              i should dependOn(f1I)
              d should dependOn(f1D)
              s should dependOn(f1S)
            }
            inside(filter2.outputAttributes) { case Seq(f2I, f2D, f2S) =>
              i should dependOn(f2I)
              d should dependOn(f2D)
              s should dependOn(f2S)
            }
          }

          inside(filter1.outputAttributes) { case Seq(i, d, s) =>
            inside(localRelation1.outputAttributes) { case Seq(lr1I, lr1D, lr1S) =>
              i should dependOn(lr1I)
              d should dependOn(lr1D)
              s should dependOn(lr1S)
            }
          }

          inside(filter2.outputAttributes) { case Seq(i, d, s) =>
            inside(localRelation2.outputAttributes) { case Seq(lr2I, lr2D, lr2S) =>
              i should dependOn(lr2I)
              d should dependOn(lr2D)
              s should dependOn(lr2S)
            }
          }

        }
      }
    }

  it should "support join operation, forming a diamond graph" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import org.apache.spark.sql.functions._
        import spark.implicits._
        val tmpLocal = TempDirectory(pathOnly = true).deleteOnExit()
        val tmpPath = tmpLocal.asString

        val initialDF = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        val filteredDF = initialDF
          .filter($"i" =!= 5)
        val aggregatedDF = initialDF
          .withColumnRenamed("i", "A")
          .groupBy($"A")
          .agg(
            min("d").as("MIN"),
            max("s").as("MAX"))

        val joinExpr = filteredDF.col("i").eqNullSafe(aggregatedDF.col("A"))
        val df = filteredDF.join(aggregatedDF, joinExpr, "inner")

        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          implicit val walker: LineageWalker = LineageWalker(plan)

          val write = plan.operations.write
          write.name should (be("InsertIntoHadoopFsRelationCommand") or be("SaveIntoDataSourceCommand"))
          write.outputSource should be(tmpLocal.toURI.toString.stripSuffix("/"))
          write.append should be(false)
          write.params should be(Map("path" -> tmpPath))
          write.extra should be(Map("destinationType" -> Some("parquet")))

          val join = write.childOperation
          join.name should be("Join")
          join.childIds.size should be(2)

          val Seq(filter, aggregate) = join.childOperations
          filter.name should be("Filter")
          filter.params should contain key "condition"

          aggregate.name should be("Aggregate")
          aggregate.params should contain key "groupingExpressions"
          aggregate.params should contain key "aggregateExpressions"

          val project = aggregate.childOperation
          project.name should be("Project")

          val localRelation1 = filter.childOperation
          localRelation1.name should be("LocalRelation")

          val localRelation2 = project.childOperation
          localRelation2.name should be("LocalRelation")

          inside(join.outputAttributes) { case Seq(i, d, s, a, min, max) =>
            inside(filter.outputAttributes) { case Seq(inI, inD, inS) =>
              i should dependOn(inI)
              d should dependOn(inD)
              s should dependOn(inS)
            }
            inside(aggregate.outputAttributes) { case Seq(inA, inMin, inMax) =>
              a should dependOn(inA)
              min should dependOn(inMin)
              max should dependOn(inMax)
            }
          }

          inside(filter.outputAttributes) { case Seq(i, d, s) =>
            inside(localRelation1.outputAttributes) { case Seq(inI, inD, inS) =>
              i should dependOn(inI)
              d should dependOn(inD)
              s should dependOn(inS)
            }
          }

          inside(aggregate.outputAttributes) { case Seq(a, min, max) =>
            inside(localRelation2.outputAttributes) { case Seq(inI, inD, inS) =>
              a should dependOn(inI)
              min should dependOn(inD)
              max should dependOn(inS)
            }
          }

        }
      }
    }

  it should "support `CREATE TABLE ... AS SELECT` in Hive" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in
    withIsolatedSparkSession(_
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
    ) { implicit spark =>
      withDatabase(s"unitTestDatabase_${this.getClass.getSimpleName}") {
        withLineageTracking { captor =>
          import spark.implicits._

          val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))

          for {
            (plan1, _) <- captor.lineageOf {
              df.createOrReplaceTempView("tempView")
              spark.sql("CREATE TABLE users_sales AS SELECT i, d, s FROM tempView ")
            }
            // Spark 3.4+ is creating 2 commands for this CTAS here so we need to ignore one
            // We only want the one that is from CreateHiveTableAsSelectCommand
            // The one we ignore here is an extra InsertIntoHiveTableCommand
            // They can come out of order so we need to filter out which one is which.
            (plan2, _) <- if (ver"$SPARK_VERSION" >= ver"3.4.0") {
              captor.lineageOf {
                Thread.sleep(5000)
              }
            } else Future[(ExecutionPlan, Seq[ExecutionEvent])](null, null)
          } yield {
            val plan = Seq(plan1, plan2)
              .filter(null.!=)
              .find(_.operations.write.name == "CreateHiveTableAsSelectCommand")
              .get

            val writeOperation = plan.operations.write
            writeOperation.id shouldEqual "op-0"
            writeOperation.append shouldEqual false
            writeOperation.childIds shouldEqual Seq("op-1")
            writeOperation.extra("destinationType") shouldEqual Some("hive")

            val otherOperations = plan.operations.other.sortBy(_.id)

            val firstOperation = otherOperations.head
            firstOperation.id shouldEqual "op-1"
            firstOperation.childIds shouldEqual Seq("op-2")
            firstOperation.name shouldEqual "Project"
            firstOperation.extra shouldBe empty

            val secondOperation = otherOperations(1)
            secondOperation.id shouldEqual "op-2"
            secondOperation.childIds shouldEqual Seq("op-3")
            secondOperation.name should (be("SubqueryAlias") or be("`tempview`")) // Spark 2.3/2.4
            secondOperation.extra shouldBe empty
          }
        }
      }
    }

  it should "collect user extra metadata" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    val injectRules =
      """
        |{
        |    "executionPlan": {
        |        "extra": { "test.extra": { "$js": "executionPlan" } }
        |    }\,
        |    "executionEvent": {
        |        "extra": { "test.extra": { "$js": "executionEvent" } }
        |    }\,
        |    "read": {
        |        "extra": { "test.extra": { "$js": "read" } }
        |    }\,
        |    "write": {
        |        "extra": { "test.extra": { "$js": "write" } }
        |    }\,
        |    "operation": {
        |        "extra": { "test.extra": { "$js": "operation" } }
        |    }
        |}
        |""".stripMargin

    withIsolatedSparkSession(_
      .config("spark.spline.postProcessingFilter", "userExtraMeta")
      .config("spark.spline.postProcessingFilter.userExtraMeta.rules", injectRules)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = TempDirectory(pathOnly = true).deleteOnExit().asString

        val dummyCSVFile = TempFile(prefix = "spline-test", suffix = ".csv").deleteOnExit().path
        FileUtils.write(dummyCSVFile.toFile, "1,2,3", "UTF-8")

        for {
          (plan, Seq(event)) <- captor.lineageOf {
            spark
              .read.csv(dummyCSVFile.toString)
              .filter($"_c0" =!= 42)
              .write.save(testDest)
          }
        } yield {
          inside(plan.operations) {
            case Operations(wop, Seq(rop), Seq(dop)) =>
              wop.extra("test.extra") should equal(wop.copy(extra = wop.extra - "test.extra"))
              rop.extra("test.extra") should equal(rop.copy(extra = rop.extra - "test.extra"))
              dop.extra("test.extra") should equal(dop.copy(extra = Map.empty))
          }

          plan.extraInfo("test.extra") should equal(plan.copy(id = None, extraInfo = plan.extraInfo - "test.extra"))
          event.extra("test.extra") should equal(event.copy(extra = event.extra - "test.extra"))
        }
      }
    }
  }

  it should "capture execution duration" in {
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = TempDirectory(pathOnly = true).deleteOnExit().asString

        for {
          (plan, Seq(event)) <- captor.lineageOf {
            spark.createDataset(Seq(TestRow(1, 2.3, "text"))).write.save(testDest)
          }
        } yield {
          plan should not be null
          event.error should be(empty)
          event.durationNs should not be empty
          event.durationNs.get should be > 0L
        }
      }
    }
  }

  it should "capture execution error" in {
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpLocal = TempDirectory(pathOnly = true).deleteOnExit()
        val tmpPath = tmpLocal.asString

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        for {
          _ <- captor.lineageOf(df.write.save(tmpPath))
          (plan, Seq(event)) <- captor.lineageOf {
            // simulate error during execution
            Try(df.write.mode(SaveMode.ErrorIfExists).save(tmpPath))
          }
        } yield {
          plan should not be null
          event.durationNs should be(empty)
          event.error should not be empty
          event.error.get.toString.toLowerCase should
            include(s"path ${tmpLocal.toURI.toString.stripSuffix("/")} already exists".toLowerCase)
        }
      }
    }
  }

  // https://github.com/AbsaOSS/spline-spark-agent/issues/39
  it should "not capture 'data'" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = TempDirectory(pathOnly = true).deleteOnExit().asString

        for {
          (plan, _) <- captor.lineageOf {
            spark.createDataset(Seq(TestRow(1, 2.3, "text"))).write.save(testDest)
          }
        } yield {

          inside(plan.operations.other.filter(_.name contains "LocalRelation")) {
            case Seq(localRelation) =>
              assert(!localRelation.params.contains("data"))
          }
        }
      }
    }

  // https://github.com/AbsaOSS/spline-spark-agent/issues/72
  it should "support lambdas" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = TempDirectory(pathOnly = true).deleteOnExit().asString

        for {
          (plan, _) <- captor.lineageOf {
            spark.createDataset(Seq(TestRow(1, 2.3, "text"))).map(_.copy(i = 2)).write.save(testDest)
          }
        } yield {
          plan.operations.other should have size 4 // LocalRelation, DeserializeToObject, Map, SerializeFromObject
          plan.operations.other(2).params should contain key "func"
        }
      }
    }
}

object LineageHarvesterSpec extends Matchers with MockitoSugar {

  case class TestRow(i: Int, d: Double, s: String)

  private val integerType = dt.Simple(UUID.fromString("e63adadc-648a-56a0-9424-3289858cf0bb"), "int", nullable = false)
  private val doubleType = dt.Simple(UUID.fromString("75fe27b9-9a00-5c7d-966f-33ba32333133"), "double", nullable = false)
  private val stringType = dt.Simple(UUID.fromString("a155e715-56ab-59c4-a94b-ed1851a6984a"), "string", nullable = true)

}
