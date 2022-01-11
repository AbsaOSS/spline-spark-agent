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
import org.scalatest.Assertion
import org.scalatest.Inside._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.commons.io.{TempDirectory, TempFile}
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.commons.version.Version._
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.OutputAttIds
import za.co.absa.spline.model.dt
import za.co.absa.spline.producer.model._
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}

import java.util.UUID
import scala.language.reflectiveCalls
import scala.util.Try

class LineageHarvesterSpec extends AsyncFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture
  with SparkDatabaseFixture {

  import za.co.absa.spline.harvester.LineageHarvesterSpec._

  it should "produce deterministic output" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val testDest = tmpDest

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

        for {
          (plan, _) <- captor.lineageOf(spark.emptyDataset[TestRow].write.save(tmpDest))
        } yield {
          inside(plan) {
            case ExecutionPlan(_, _, _, _, Operations(_, None, Some(Seq(op))), _, _, _, _, _) =>
              op.id should be("op-1")
              op.name should be(Some("LocalRelation"))
              op.childIds should be(None)
              op.output should not be None
              op.extra should be(empty)
          }
        }
      }
    }

  it should "support simple non-empty data frame" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpPath = tmpDest

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))

        val expectedAttributes = Seq(
          Attribute("attr-0", Some(integerType.id), None, None, "i"),
          Attribute("attr-1", Some(doubleType.id), None, None, "d"),
          Attribute("attr-2", Some(stringType.id), None, None, "s"))

        val expectedOperations = Operations(
          WriteOperation(
            id = "op-0",
            name = Some("InsertIntoHadoopFsRelationCommand"),
            childIds = Seq("op-1"),
            outputSource = s"file:$tmpPath",
            append = false,
            params = Map("path" -> tmpPath).asOption,
            extra = Map("destinationType" -> Some("parquet")).asOption
          ),
          None,
          Seq(
            DataOperation(
              id = "op-1",
              name = Some("LocalRelation"),
              childIds = None,
              output = expectedAttributes.map(_.id).asOption,
              params = Map("isStreaming" -> Some(false)).asOption,
              extra = None)
          ).asOption
        )

        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          plan.operations shouldEqual expectedOperations
          plan.attributes shouldEqual Some(expectedAttributes)
        }
      }
    }

  it should "support filter and column renaming operations" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpPath = tmpDest

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
          .withColumnRenamed("i", "A")
          .filter($"A".notEqual(5))

        val expectedAttributes = Seq(
          Attribute("attr-0", Some(integerType.id), None, None, "i"),
          Attribute("attr-1", Some(doubleType.id), None, None, "d"),
          Attribute("attr-2", Some(stringType.id), None, None, "s"),
          Attribute("attr-3", Some(integerType.id), Seq(AttrOrExprRef(None, Some("expr-0"))).asOption, None, "A")
        )

        val outputBeforeRename = Seq("attr-0", "attr-1", "attr-2")
        val outputAfterRename = Seq("attr-3", "attr-1", "attr-2")

        val expectedOperations = Operations(
          WriteOperation(
            id = "op-0",
            name = Some("InsertIntoHadoopFsRelationCommand"),
            childIds = Seq("op-1"),
            outputSource = s"file:$tmpPath",
            append = false,
            params = Map("path" -> tmpPath).asOption,
            extra = Map("destinationType" -> Some("parquet")).asOption
          ),
          None,
          Seq(
            DataOperation(
              "op-3", Some("LocalRelation"), None, outputBeforeRename.asOption,
              params = Map("isStreaming" -> Some(false)).asOption,
              None),
            DataOperation(
              "op-2", Some("Project"), Seq("op-3").asOption, outputAfterRename.asOption,
              params = Map(
                "projectList" -> Seq(
                  AttrOrExprRef(None, Some("expr-0")),
                  AttrOrExprRef(Some("attr-1"), None),
                  AttrOrExprRef(Some("attr-2"), None)
                ).asOption
              ).asOption,
              None),
            DataOperation(
              "op-1", Some("Filter"), Seq("op-2").asOption, outputAfterRename.asOption,
              params = Map("condition" -> Some(AttrOrExprRef(None, Some("expr-1")))).asOption,
              None)
          ).asOption
        )

        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          plan.operations shouldEqual expectedOperations
          plan.attributes shouldEqual Some(expectedAttributes)
        }
      }
    }

  it should "support union operation, forming a diamond graph" taggedAs ignoreIf(ver"$SPARK_VERSION" >= ver"3.0.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val tmpPath = tmpDest

        val initialDF = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        val filteredDF1 = initialDF.filter($"i".notEqual(5))
        val filteredDF2 = initialDF.filter($"d".notEqual(5.0))
        val df = filteredDF1.union(filteredDF2)

        val expectedAttributes =
          Seq(
            Attribute("attr-0", Some(integerType.id), None, None, "i"),
            Attribute("attr-1", Some(doubleType.id), None, None, "d"),
            Attribute("attr-2", Some(stringType.id), None, None, "s"),
            Attribute("attr-3", Some(integerType.id), None, None, "i"),
            Attribute("attr-4", Some(doubleType.id), None, None, "d"),
            Attribute("attr-5", Some(stringType.id), None, None, "s"))

        val inputAttIds = Seq("attr-0", "attr-1", "attr-2")
        val outputAttIds = Seq("attr-3", "attr-4", "attr-5")

        val expectedOperations = Operations(
          WriteOperation(
            id = "op-0",
            name = Some("InsertIntoHadoopFsRelationCommand"),
            childIds = Seq("op-1"),
            outputSource = s"file:$tmpPath",
            append = false,
            params = Map("path" -> tmpPath).asOption,
            extra = Map("destinationType" -> Some("parquet")).asOption
          ),
          None,
          Seq(
            DataOperation("op-1", Some("Union"), Seq("op-2", "op-4").asOption, outputAttIds.asOption, None, None),
            DataOperation("op-2", Some("Filter"), Seq("op-3").asOption, inputAttIds.asOption, None, None),
            DataOperation("op-4", Some("Filter"), Seq("op-3").asOption, inputAttIds.asOption, None, None),
            DataOperation("op-3", Some("LocalRelation"), None, inputAttIds.asOption, None, None)
          ).asOption
        )
        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          plan.operations shouldEqual expectedOperations
          plan.attributes shouldEqual Some(expectedAttributes)
        }
      }
    }

  it should "support join operation, forming a diamond graph" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import org.apache.spark.sql.functions._
        import spark.implicits._
        val tmpPath = tmpDest

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

        val expectedAttributes = Seq(
          Attribute("attr-0", Some(integerType.id), None, None, "i"),
          Attribute("attr-1", Some(doubleType.id), None, None, "d"),
          Attribute("attr-2", Some(stringType.id), None, None, "s"),
          Attribute("attr-3", Some(integerType.id), None, None, "A"),
          Attribute("attr-4", Some(doubleNullableType.id), Seq(AttrOrExprRef(None,Some("expr-4"))).asOption, None, "MIN"),
          Attribute("attr-5", Some(stringType.id), Seq(AttrOrExprRef(None,Some("expr-7"))).asOption, None, "MAX"))

        val expectedOperations = Operations(
          WriteOperation(
            id = "op-0",
            name = Some("InsertIntoHadoopFsRelationCommand"),
            childIds = Seq("op-1"),
            outputSource = s"file:$tmpPath",
            append = false,
            params = Map("path" -> tmpPath).asOption,
            extra = Map("destinationType" -> Some("parquet")).asOption
          ),
          None,
          Seq(
            DataOperation(
              "op-3", Some("LocalRelation"), None, Seq("attr-0", "attr-1", "attr-2").asOption,
              params = Map("isStreaming" -> Some(false)).asOption,
              None),
            DataOperation(
              "op-2", Some("Filter"), Seq("op-3").asOption, Seq("attr-0", "attr-1", "attr-2").asOption,
              None, None),
            DataOperation(
              "op-5", Some("Project"), Seq("op-3").asOption, Seq("attr-3", "attr-1", "attr-2").asOption,
              None, None),
            DataOperation(
              "op-4", Some("Aggregate"), Seq("op-5").asOption, Seq("attr-3", "attr-4", "attr-5").asOption,
              None, None),
            DataOperation(
              "op-1", Some("Join"), Seq("op-2", "op-4").asOption, Seq("attr-0", "attr-1", "attr-2", "attr-3", "attr-4", "attr-5").asOption,
              Map("joinType" -> Some("INNER")).asOption, None),

          ).asOption
        )
        for {
          (plan, _) <- captor.lineageOf(df.write.save(tmpPath))
        } yield {
          plan.operations shouldEqual expectedOperations
          plan.attributes shouldEqual Some(expectedAttributes)
        }
      }
    }

  it should "support `CREATE TABLE ... AS SELECT` in Hive" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in
    withRestartingSparkContext {
      withCustomSparkSession(_
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
      ) { implicit spark =>
        withDatabase(s"unitTestDatabase_${this.getClass.getSimpleName}") {
          withLineageTracking { captor =>
            import spark.implicits._

            val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))

            for {
              (plan, _) <- captor.lineageOf {
                df.createOrReplaceTempView("tempView")
                spark.sql("create table users_sales as select i, d, s from tempView ")
              }
            } yield {
              val writeOperation = plan.operations.write
              writeOperation.id shouldEqual "op-0"
              writeOperation.append shouldEqual false
              writeOperation.childIds shouldEqual Seq("op-1")
              writeOperation.extra.get("destinationType") shouldEqual Some("hive")

              val otherOperations = plan.operations.other.get.sortBy(_.id)

              val firstOperation = otherOperations(0)
              firstOperation.id shouldEqual "op-1"
              firstOperation.childIds.get shouldEqual Seq("op-2")
              firstOperation.name shouldEqual Some("Project")
              firstOperation.extra shouldBe empty

              val secondOperation = otherOperations(1)
              secondOperation.id shouldEqual "op-2"
              secondOperation.childIds.get shouldEqual Seq("op-3")
              secondOperation.name should (equal(Some("SubqueryAlias")) or equal(Some("`tempview`"))) // Spark 2.3/2.4
              secondOperation.extra shouldBe empty

              val thirdOperation = otherOperations(2)
              thirdOperation.id shouldEqual "op-3"
              thirdOperation.childIds shouldEqual None
              thirdOperation.name shouldEqual Some("LocalRelation")
              thirdOperation.extra shouldBe empty
            }
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

    withCustomSparkSession(_
      .config("spark.spline.postProcessingFilter", "userExtraMeta")
      .config("spark.spline.postProcessingFilter.userExtraMeta.rules", injectRules)
    ) { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        val dummyCSVFile = TempFile(prefix = "spline-test", suffix = ".csv").deleteOnExit().path
        FileUtils.write(dummyCSVFile.toFile, "1,2,3", "UTF-8")

        for {
          (plan, Seq(event)) <- captor.lineageOf {
            spark
              .read.csv(dummyCSVFile.toString)
              .filter($"_c0" =!= 42)
              .write.save(tmpDest)
          }
        } yield {
          inside(plan.operations) {
            case Operations(wop, Some(Seq(rop)), Some(Seq(dop))) =>
              wop.extra.get("test.extra") should equal(wop.copy(extra = Some(wop.extra.get - "test.extra")))
              rop.extra.get("test.extra") should equal(rop.copy(extra = Some(rop.extra.get - "test.extra")))
              dop.extra.get("test.extra") should equal(dop.copy(extra = None))
          }

          plan.extraInfo.get("test.extra") should equal(plan.copy(id = None, extraInfo = Some(plan.extraInfo.get - "test.extra")))
          event.extra.get("test.extra") should equal(event.copy(extra = Some(event.extra.get - "test.extra")))
        }
      }
    }
  }

  it should "capture execution duration" in {
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._
        val dest = tmpDest
        for {
          (plan, Seq(event)) <- captor.lineageOf {
            spark.createDataset(Seq(TestRow(1, 2.3, "text"))).write.save(dest)
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
        val dest = tmpDest
        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        for {
          _ <- captor.lineageOf(df.write.save(dest))
          (plan, Seq(event)) <- captor.lineageOf {
            // simulate error during execution
            Try(df.write.mode(SaveMode.ErrorIfExists).save(dest))
          }
        } yield {
          plan should not be null
          event.durationNs should be(empty)
          event.error should not be empty
          event.error.get.toString should include(s"path file:$dest already exists")
        }
      }
    }
  }

  // https://github.com/AbsaOSS/spline-spark-agent/issues/39
  it should "not capture 'data'" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        for {
          (plan, _) <- captor.lineageOf {
            spark.createDataset(Seq(TestRow(1, 2.3, "text"))).write.save(tmpDest)
          }
        } yield {

          inside(plan.operations.other.toSeq.flatten.filter(_.name contains "LocalRelation")) {
            case Seq(localRelation) =>
              assert(localRelation.params.forall(p => !p.contains("data")))
          }
        }
      }
    }

  // https://github.com/AbsaOSS/spline-spark-agent/issues/72
  it should "support lambdas" in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        import spark.implicits._

        for {
          (plan, _) <- captor.lineageOf {
            spark.createDataset(Seq(TestRow(1, 2.3, "text"))).map(_.copy(i = 2)).write.save(tmpDest)
          }
        } yield {
          plan.operations.other.get should have size 4 // LocalRelation, DeserializeToObject, Map, SerializeFromObject
          plan.operations.other.get(2).params.get should contain key "func"
        }
      }
    }
}

object LineageHarvesterSpec extends Matchers with MockitoSugar {

  case class TestRow(i: Int, d: Double, s: String)

  private def tmpDest: String = TempDirectory(pathOnly = true).deleteOnExit().path.toString

  private val integerType = dt.Simple(UUID.fromString("e63adadc-648a-56a0-9424-3289858cf0bb"), "int", nullable = false)
  private val doubleType = dt.Simple(UUID.fromString("75fe27b9-9a00-5c7d-966f-33ba32333133"), "double", nullable = false)
  private val doubleNullableType = dt.Simple(UUID.fromString("455d9d5b-7620-529e-840b-897cee45e560"), "double", nullable = true)
  private val stringType = dt.Simple(UUID.fromString("a155e715-56ab-59c4-a94b-ed1851a6984a"), "string", nullable = true)

  implicit class ReferenceMatchers(outputAttIds: OutputAttIds) {

    import ReferenceMatchers._

    def shouldReference(references: Seq[Attribute]) = new ReferenceMatcher[Attribute](outputAttIds, references)

    def references(references: Seq[Attribute]) = new ReferenceMatcher[Attribute](outputAttIds, references)
  }

  object ReferenceMatchers {

    import scala.language.reflectiveCalls

    type ID = String
    type Refs = Seq[ID]

    class ReferenceMatcher[A <: {def id: ID}](val refs: Refs, val attributes: Seq[A]) {
      private lazy val targets = attributes.map(_.id)

      private def referencePositions = refs.map(targets.indexOf)

      def as(anotherComparator: ReferenceMatcher[A]): Assertion = {
        referencePositions shouldEqual anotherComparator.referencePositions
      }
    }

  }

}
