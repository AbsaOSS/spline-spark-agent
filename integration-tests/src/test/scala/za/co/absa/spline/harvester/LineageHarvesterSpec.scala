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

import java.util.UUID
import java.util.UUID.randomUUID
import org.apache.commons.io.FileUtils
import org.apache.spark.SPARK_VERSION
import org.scalatest.Assertion
import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.{TempDirectory, TempFile}
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.commons.version.Version._
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.OutputAttIds
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.extra.UserExtraMetadataProvider
import za.co.absa.spline.model.dt
import za.co.absa.spline.producer.model.v1_1.{Attribute, _}
import za.co.absa.spline.test.fixture.spline.SplineFixture.EMPTY_CONF
import za.co.absa.spline.test.fixture.spline.{LineageCaptor, LineageCapturingDispatcher, SplineFixture}
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}

import scala.language.reflectiveCalls

class LineageHarvesterSpec extends AnyFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture
  with SparkDatabaseFixture {


  import za.co.absa.spline.harvester.LineageHarvesterSpec._

  "When harvest method is called with an empty data frame" should "return a data lineage with one node." in
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import spark.implicits._

        inside(lineageOf(spark.emptyDataset[TestRow].write.save(tmpDest))) {
          case (ExecutionPlan(_, Operations(_, None, Some(Seq(op))), _, _, _, _, _), _) =>
            op.id should be("1")
            op.childIds should be(None)
            op.output should not be None
            op.extra.get should contain("name" -> "LocalRelation")
        }
      }
    })

  "When harvest method is called with a simple non-empty data frame" should "return a data lineage with one node." in
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import spark.implicits._

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))

        val expectedAttributes = Seq(
          Attribute(randomUUID.toString, Some(integerType.id), None, None, "i"),
          Attribute(randomUUID.toString, Some(doubleType.id), None, None, "d"),
          Attribute(randomUUID.toString, Some(stringType.id), None, None, "s"))

        val expectedOperations = Seq(
          WriteOperation(
            id = "0",
            childIds = Seq("1"),
            outputSource = s"file:$tmpDest",
            append = false,
            params = None,
            extra = None,
            output = expectedAttributes.map(_.id)
          ),
          DataOperation(
            id = "1",
            childIds = None,
            output = expectedAttributes.map(_.id),
            params = None,
            extra = Map("name" -> "LocalRelation").asOption))

        val (plan, _) = lineageOf(df.write.save(tmpDest))

        assertDataLineage(expectedOperations, expectedAttributes, plan)
      }
    })

  "When harvest method is called with a filtered data frame" should "return a data lineage forming a path with three nodes." in
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import spark.implicits._

        val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
          .withColumnRenamed("i", "A")
          .filter($"A".notEqual(5))

        val expectedAttributes = Seq(
          Attribute(randomUUID.toString, Some(integerType.id), None, None, "i"),
          Attribute(randomUUID.toString, Some(doubleType.id), None, None, "d"),
          Attribute(randomUUID.toString, Some(stringType.id), None, None, "s"),
          Attribute(randomUUID.toString, Some(integerType.id), None, None, "A")
        )

        val outputBeforeRename = Seq(expectedAttributes(0).id, expectedAttributes(1).id, expectedAttributes(2).id)
        val outputAfterRename = Seq(expectedAttributes(3).id, expectedAttributes(1).id, expectedAttributes(2).id)

        val expectedOperations = Seq(
          WriteOperation(
            id = "0",
            childIds = Seq("1"),
            outputSource = s"file:$tmpDest",
            append = false,
            params = None,
            extra = None,
            output = outputAfterRename
          ),
          DataOperation(
            "1", Seq("2").asOption, outputAfterRename,
            None, Map("name" -> "Filter").asOption),
          DataOperation(
            "2", Seq("3").asOption, outputAfterRename,
            None, Map("name" -> "Project").asOption),
          DataOperation(
            "3", None, outputBeforeRename,
            None, Map("name" -> "LocalRelation").asOption))

        val (plan, _) = lineageOf(df.write.save(tmpDest))

        assertDataLineage(expectedOperations, expectedAttributes, plan)
      }
    })

  "When harvest method is called with an union data frame" should "return a data lineage forming a diamond graph." in
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import spark.implicits._

        val initialDF = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        val filteredDF1 = initialDF.filter($"i".notEqual(5))
        val filteredDF2 = initialDF.filter($"d".notEqual(5))
        val df = filteredDF1.union(filteredDF2)

        val expectedAttributes =
          Seq(
            Attribute(randomUUID.toString, Some(integerType.id), None, None, "i"),
            Attribute(randomUUID.toString, Some(doubleType.id), None, None, "d"),
            Attribute(randomUUID.toString, Some(stringType.id), None, None, "s"),
            Attribute(randomUUID.toString, Some(integerType.id), None, None, "union of i, i"),
            Attribute(randomUUID.toString, Some(doubleType.id), None, None, "union of d, d"),
            Attribute(randomUUID.toString, Some(stringType.id), None, None, "union of s, s")
          )

        val inputAttIds = expectedAttributes.slice(0, 3).map(_.id)
        val outputAttIds = expectedAttributes.slice(3, 6).map(_.id)

        val expectedOperations = Seq(
          WriteOperation(
            id = "0",
            childIds = Seq("1"),
            outputSource = s"file:$tmpDest",
            append = false,
            params = None,
            extra = None,
            output = outputAttIds
          ),
          DataOperation("1", Seq("2", "4").asOption, outputAttIds, None, Map("name" -> "Union").asOption),
          DataOperation("2", Seq("3").asOption, inputAttIds, None, Map("name" -> "Filter").asOption),
          DataOperation("4", Seq("3").asOption, inputAttIds, None, Map("name" -> "Filter").asOption),
          DataOperation("3", None, inputAttIds, None, Map("name" -> "LocalRelation").asOption))

        val (plan, _) = lineageOf(df.write.save(tmpDest))

        assertDataLineage(expectedOperations, expectedAttributes, plan)
      }
    })

  "When harvest method is called with a joined data frame" should "return a data lineage forming a diamond graph." in
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import org.apache.spark.sql.functions._
        import spark.implicits._

        val initialDF = spark.createDataset(Seq(TestRow(1, 2.3, "text")))
        val filteredDF = initialDF
          .filter($"i" =!= 5)
        val aggregatedDF = initialDF
          .withColumnRenamed("i", "A")
          .groupBy($"A")
          .agg(
            min("d").as("MIN"),
            max("s").as("MAX"))

        val df = filteredDF.join(aggregatedDF, filteredDF.col("i").eqNullSafe(aggregatedDF.col("A")), "inner")

        val expectedAttributes = Seq(
          Attribute(randomUUID.toString, Some(integerType.id), None, None, "i"),
          Attribute(randomUUID.toString, Some(doubleType.id), None, None, "d"),
          Attribute(randomUUID.toString, Some(stringType.id), None, None, "s"),
          Attribute(randomUUID.toString, Some(integerType.id), None, None, "A"),
          Attribute(randomUUID.toString, Some(doubleNullableType.id), None, None, "MIN"),
          Attribute(randomUUID.toString, Some(stringType.id), None, None, "MAX"))

        val expectedOperations = Seq(
          WriteOperation(
            id = "0",
            childIds = Seq("1"),
            outputSource = s"file:$tmpDest",
            append = false,
            params = None,
            extra = None,
            output = expectedAttributes.map(_.id)
          ),
          DataOperation(
            "1", Seq("2", "4").asOption, expectedAttributes.map(_.id),
            Map("joinType" -> Some("INNER")).asOption,
            Map("name" -> "Join").asOption),
          DataOperation(
            "2", Seq("3").asOption, Seq(expectedAttributes(0).id, expectedAttributes(1).id, expectedAttributes(2).id),
            None, Map("name" -> "Filter").asOption),
          DataOperation(
            "3", None, Seq(expectedAttributes(0).id, expectedAttributes(1).id, expectedAttributes(2).id),
            None, Map("name" -> "LocalRelation").asOption),
          DataOperation(
            "4", Seq("5").asOption, Seq(expectedAttributes(3).id, expectedAttributes(4).id, expectedAttributes(5).id),
            None, Map("name" -> "Aggregate").asOption),
          DataOperation(
            "5", Seq("3").asOption, Seq(expectedAttributes(3).id, expectedAttributes(1).id, expectedAttributes(2).id),
            None, Map("name" -> "Project").asOption))

        val (plan, _) = lineageOf(df.write.save(tmpDest))

        assertDataLineage(expectedOperations, expectedAttributes, plan)
      }
    })

  "Create table as" should "produce lineage when creating a Hive table from temp view" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in
    withRestartingSparkContext {
      withCustomSparkSession(_
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition.mode", "nonstrict")) { spark =>

        val databaseName = s"unitTestDatabase_${this.getClass.getSimpleName}"
        withHiveDatabase(spark)(databaseName) {

          val (plan, _) = withLineageTracking(spark)(lineageCaptor => {

            import spark.implicits._

            val df = spark.createDataset(Seq(TestRow(1, 2.3, "text")))

            lineageCaptor.lineageOf {
              df.createOrReplaceTempView("tempView")
              spark.sql("create table users_sales as select i, d, s from tempView ")
            }
          })

          val writeOperation = plan.operations.write
          writeOperation.id shouldEqual "0"
          writeOperation.append shouldEqual false
          writeOperation.childIds shouldEqual Seq("1")
          writeOperation.extra.get("destinationType") shouldEqual Some("hive")

          val otherOperations = plan.operations.other.get.sortBy(_.id)

          val firstOperation = otherOperations(0)
          firstOperation.id shouldEqual "1"
          firstOperation.childIds.get shouldEqual Seq("2")
          firstOperation.extra.get("name") shouldEqual "Project"

          val secondOperation = otherOperations(1)
          secondOperation.id shouldEqual "2"
          secondOperation.childIds.get shouldEqual Seq("3")
          secondOperation.extra.get("name") should (equal("SubqueryAlias") or equal(Some("`tempview`"))) // Spark 2.3/2.4

          val thirdOperation = otherOperations(2)
          thirdOperation.id shouldEqual "3"
          thirdOperation.childIds shouldEqual None
          thirdOperation.extra.get("name") shouldEqual "LocalRelation"
        }
      }
    }

  "harvest()" should "collect user extra metadata" in {
    withNewSparkSession(spark => {
      val lineageCaptor = new LineageCaptor

      val testSplineConfigurer = new DefaultSplineConfigurer(spark, EMPTY_CONF) {
        override protected def lineageDispatcher: LineageDispatcher = new LineageCapturingDispatcher(lineageCaptor.setter)

        override protected def maybeUserExtraMetadataProvider: Option[UserExtraMetadataProvider] = Some(
          new UserExtraMetadataProvider {
            override def forExecEvent(event: ExecutionEvent, ctx: HarvestingContext): Map[String, Any] =
              Map("test.extra" -> event)

            override def forExecPlan(plan: ExecutionPlan, ctx: HarvestingContext): Map[String, Any] =
              Map("test.extra" -> plan)

            override def forOperation(op: ReadOperation, ctx: HarvestingContext): Map[String, Any] =
              Map("test.extra" -> op)

            override def forOperation(op: WriteOperation, ctx: HarvestingContext): Map[String, Any] =
              Map("test.extra" -> op)

            override def forOperation(op: DataOperation, ctx: HarvestingContext): Map[String, Any] =
              Map("test.extra" -> op)
          })
      }

      import SparkLineageInitializer._
      import spark.implicits._
      spark.enableLineageTracking(testSplineConfigurer)

      val dummyCSVFile = TempFile(prefix = "spline-test", suffix = ".csv").deleteOnExit().path
      FileUtils.write(dummyCSVFile.toFile, "1,2,3", "UTF-8")

      val (plan, Seq(event)) = lineageCaptor.getter.lineageOf {
        spark
          .read.csv(dummyCSVFile.toString)
          .filter($"_c0" =!= 42)
          .write.save(tmpDest)
      }

      inside(plan.operations) {
        case Operations(wop, Some(Seq(rop)), Some(Seq(dop))) =>
          wop.extra.get("test.extra") should equal(wop.copy(extra = Some(wop.extra.get - "test.extra")))
          rop.extra.get("test.extra") should equal(rop.copy(extra = Some(rop.extra.get - "test.extra")))
          dop.extra.get("test.extra") should equal(dop.copy(extra = Some(dop.extra.get - "test.extra")))
      }

      plan.extraInfo.get("test.extra") should equal(plan.copy(extraInfo = Some(plan.extraInfo.get - "test.extra")))
      event.extra.get("test.extra") should equal(event.copy(extra = Some(event.extra.get - "test.extra")))
    })
  }

  // https://github.com/AbsaOSS/spline-spark-agent/issues/39
  it should "not capture 'data'" in {
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import spark.implicits._

        val (plan, _) = lineageOf {
          spark.createDataset(Seq(TestRow(1, 2.3, "text"))).write.save(tmpDest)
        }

        val localRelation = plan.operations.other.get.find(_.extra.get("name") == "LocalRelation").get
        assert(localRelation.params.forall(p => !p.contains("data")))
      }
    })
  }

  // https://github.com/AbsaOSS/spline-spark-agent/issues/72
  it should "support lambdas" in {
    withNewSparkSession(spark => {
      withLineageTracking(spark) { lineageCaptor =>
        import lineageCaptor._
        import spark.implicits._

        val (plan, _) = lineageOf {
          spark.createDataset(Seq(TestRow(1, 2.3, "text"))).map(_.copy(i = 2)).write.save(tmpDest)
        }

        plan.operations.other.get should have size 4 // LocalRelation, DeserializeToObject, Map, SerializeFromObject
        plan.operations.other.get(2).params.get should contain key "func"
      }
    })
  }

}

object LineageHarvesterSpec extends Matchers {

  case class TestRow(i: Int, d: Double, s: String)

  private def tmpDest: String = TempDirectory(pathOnly = true).deleteOnExit().path.toString

  private val integerType = dt.Simple("integer", nullable = false)
  private val doubleType = dt.Simple("double", nullable = false)
  private val doubleNullableType = dt.Simple("double", nullable = true)
  private val stringType = dt.Simple("string", nullable = true)
  private val testTypesById = Seq(
    integerType,
    doubleType,
    doubleNullableType,
    stringType)
    .map(t => t.id -> t).toMap

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

  implicit class OperationsAdapter(op: Operations) {
    def all: Seq[OperationsAdapter.OperationLike] =
      Seq(
        Seq(op.write),
        op.reads getOrElse Nil,
        op.other getOrElse Nil
      ).flatten.map(_.asInstanceOf[OperationsAdapter.OperationLike])
  }

  object OperationsAdapter {
    type OperationLike = {
      def id: String
      def childIds: Any
      def output: Seq[String]
      def params: Option[Map[String, Any]]
      def extra: Option[Map[String, Any]]
    }

    implicit class OperationLikeAdapter(op: OperationLike) {
      def childIdList: Seq[_] = op.childIds match {
        case ids: Seq[_] => ids
        case Some(ids: Seq[_]) => ids
        case _ => Nil
      }
    }

  }

  def assertDataLineage(
    expectedOperations: Seq[AnyRef],
    expectedAttributes: Seq[Attribute],
    actualPlan: ExecutionPlan): Unit = {

    import za.co.absa.spline.harvester.LineageHarvesterSpec.OperationsAdapter._

    actualPlan.operations shouldNot be(null)

    val actualAttributes = actualPlan.attributes.get
    val actualDataTypes = actualPlan.extraInfo.get("dataTypes").asInstanceOf[Seq[dt.DataType]].map(t => t.id -> t).toMap

    val actualOperationsSorted = actualPlan.operations.all.sortBy(x => x.id)
    val expectedOperationsSorted = expectedOperations.map(_.asInstanceOf[OperationLike]).sortBy(_.id)

    for ((opActual, opExpected) <- actualOperationsSorted.zip(expectedOperationsSorted)) {
      opActual.childIdList should contain theSameElementsInOrderAs opExpected.childIdList
      opActual.id should be(opExpected.id)
      for (expectedParams <- opExpected.params) opActual.params.get should contain allElementsOf expectedParams
      for (expectedExtra <- opExpected.extra) opActual.extra.get should contain allElementsOf expectedExtra


      val actualOutput = opActual.output
      val expectedOutput = opExpected.output
      actualOutput shouldReference actualAttributes as (expectedOutput references expectedAttributes)

      opActual.output.size shouldEqual opExpected.output.size
    }

    for ((attrActual: Attribute, attrExpected: Attribute) <- actualAttributes.zip(expectedAttributes)) {
      attrActual.name shouldEqual attrExpected.name
      val typeActual = actualDataTypes(attrActual.dataType.get.asInstanceOf[UUID])
      val typeExpected = testTypesById(attrExpected.dataType.get.asInstanceOf[UUID])
      inside(typeActual) {
        case dt.Simple(_, typeName, nullable) =>
          typeName should be(typeExpected.name)
          nullable should be(typeExpected.nullable)
      }
    }
  }

}
