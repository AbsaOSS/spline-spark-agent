/*
 * Copyright 2020 ABSA Group Limited
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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.scalatest.OptionValues._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.commons.version.Version.VersionStringInterpolator
import za.co.absa.spline.producer.model.{ExprRef, ReadOperation}
import za.co.absa.spline.test.LineageWalker
import za.co.absa.spline.test.ProducerModelImplicits._
import za.co.absa.spline.test.SplineMatchers._
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}

class DeltaDSV2Spec extends AsyncFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture
  with SparkDatabaseFixture {

  it should "support AppendData V2 command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE foo (ID int, NAME string) USING delta")
              testData.write.format("delta").mode("append").saveAsTable("foo")
            }
          } yield {
            plan1.id.value shouldEqual event1.planId
            plan1.operations.write.append shouldBe true
            plan1.operations.write.extra("destinationType") shouldBe Some("delta")
            plan1.operations.write.outputSource should endWith("/testdb.db/foo")
          }
        }
      }
    }

  it should "support OverwriteByExpression V2 command without deleteExpression" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE foo (ID int, NAME string) USING delta")
              testData.write.format("delta").mode("overwrite").insertInto("foo")
            }
          } yield {
            plan1.id.value shouldEqual event1.planId
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("delta")
            val deleteExprId = plan1.operations.write.params("deleteExpr").asInstanceOf[ExprRef].id
            val literal = plan1.expressions.constants.find(_.id == deleteExprId).value
            literal.value shouldEqual true
            plan1.operations.write.outputSource should endWith("/testdb.db/foo")
          }
        }
      }
    }

  it should "support OverwriteByExpression V2 command with deleteExpression" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }
          testData.createOrReplaceTempView("tempdata")

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE foo (ID int, NAME string) USING delta PARTITIONED BY (ID)")
              spark.sql(
                """
                  |INSERT OVERWRITE foo PARTITION (ID = 222222)
                  |  (SELECT NAME FROM tempdata WHERE NAME = 'Warsaw')
                  |""".stripMargin)
            }
          } yield {
            plan1.id.value shouldEqual event1.planId
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("delta")
            plan1.operations.write.params("deleteExpr") should not be Literal(true)
            plan1.operations.write.outputSource should endWith("/testdb.db/foo")
          }
        }
      }
    }

  /**
    * Even though the code actually does dynamic partition overwrite,
    * the spark command generated is OverwriteByExpression.
    * Keeping this test in case the command will be used in future Spark versions.
    */
  it should "support OverwritePartitionsDynamic V2 command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }
          testData.createOrReplaceTempView("tempdata")

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE foo (ID int, NAME string) USING delta PARTITIONED BY (NAME)")
              spark.sql(
                """
                  |INSERT OVERWRITE foo PARTITION (NAME)
                  |  (SELECT ID, NAME FROM tempdata WHERE NAME = 'Warsaw')
                  |""".stripMargin)
            }
          } yield {
            plan1.id.value shouldEqual event1.planId
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("delta")
            plan1.operations.write.outputSource should endWith("/testdb.db/foo")
          }
        }
      }
    }

  it should "support CreateTableAsSelect V2 command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              testData.write.format("delta").saveAsTable("foo")
            }
          } yield {
            plan1.id.value shouldEqual event1.planId
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("delta")
            plan1.operations.write.outputSource should endWith("/testdb.db/foo")
          }
        }
      }
    }

  it should "support ReplaceTableAsSelect V2 command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE foo (toBeOrNotToBe boolean) USING delta")
              testData.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("foo")
            }
          } yield {
            plan1.id.value shouldEqual event1.planId
            plan1.operations.write.append shouldBe false
            plan1.operations.write.extra("destinationType") shouldBe Some("delta")
            plan1.operations.write.outputSource should endWith("/testdb.db/foo")
          }
        }
      }
    }

  it should "support DELETE table command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              testData.write.format("delta").saveAsTable("foo")
            }
            (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
              spark.sql("DELETE FROM foo WHERE ID == 1014")
            }
          } yield {
            plan2.id.value shouldEqual event2.planId
            plan2.operations.write.append shouldBe false
            plan2.operations.write.extra("destinationType") shouldBe Some("delta")
            plan2.operations.write.outputSource should endWith("/testdb.db/foo")
            plan2.operations.write.params("condition").asInstanceOf[String] should include("1014")
            plan2.operations.reads.head.output.size shouldBe 2
          }
        }
      }
    }

  it should "support UPDATE table command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          val testData = {
            import spark.implicits._
            Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
          }

          for {
            (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
              testData.write.format("delta").saveAsTable("foo")
            }
            (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
              spark.sql("UPDATE foo SET NAME = 'Korok' WHERE ID == 1002")
            }
          } yield {
            plan2.id.value shouldEqual event2.planId
            plan2.operations.write.append shouldBe false
            plan2.operations.write.extra("destinationType") shouldBe Some("delta")
            plan2.operations.write.outputSource should endWith("/testdb.db/foo")
            plan2.operations.write.params("condition").asInstanceOf[String] should include("1002")
            plan2.operations.write.params("updateExpressions").asInstanceOf[Seq[String]] should contain("Korok")
            plan2.operations.reads.head.output.size shouldBe 2
          }
        }
      }
    }

  it should "support MERGE INTO table command" taggedAs
    ignoreIf(ver"$SPARK_VERSION" < ver"3.0.0") in
    withIsolatedSparkSession(_
      .enableHiveSupport
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ) { implicit spark =>
      withLineageTracking { lineageCaptor =>
        withDatabase("testDB") {
          for {
            (_, _) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE foo ( ID int, NAME string ) USING DELTA")
              spark.sql("INSERT INTO foo VALUES (1014, 'Warsaw'), (1002, 'Corte')")
            }
            (_, _) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE fooUpdate ( ID Int, NAME String ) USING DELTA")
              spark.sql("INSERT INTO fooUpdate VALUES (1014, 'Lodz'), (1003, 'Prague')")
            }
            (_, _) <- lineageCaptor.lineageOf {
              spark.sql("CREATE TABLE barUpdate ( ID Int, NAME String ) USING DELTA")
              spark.sql("INSERT INTO barUpdate VALUES (4242, 'Paris'), (3342, 'Bordeaux')")
            }
            (plan, Seq(event)) <- lineageCaptor.lineageOf {
              spark.sql(
                """
                  | CREATE OR REPLACE VIEW tempview AS
                  |   SELECT * FROM fooUpdate
                  |   UNION
                  |   SELECT * FROM barUpdate
                  |""".stripMargin
              )

              spark.sql(
                """
                  | MERGE INTO foo
                  | USING tempview AS foobar
                  | ON foo.ID = foobar.ID
                  | WHEN MATCHED THEN
                  |   UPDATE SET
                  |     NAME = foobar.NAME
                  | WHEN NOT MATCHED
                  |  THEN INSERT (ID, NAME)
                  |  VALUES (foobar.ID, foobar.NAME)
                  |""".stripMargin
              )
            }
          } yield {
            implicit val walker = LineageWalker(plan)

            plan.id.value shouldEqual event.planId
            plan.operations.write.append shouldBe false
            plan.operations.write.extra("destinationType") shouldBe Some("delta")
            plan.operations.write.outputSource should endWith("/testdb.db/foo")

            val mergeOp = plan.operations.write.childOperation
            mergeOp.params("condition").asInstanceOf[Option[String]].value should include("ID")

            val reads =  plan.operations.reads

            val mergeOutput = mergeOp.outputAttributes
            val read0Output = reads(0).outputAttributes
            val read1Output = reads(1).outputAttributes
            val read2Output = reads(2).outputAttributes

            mergeOutput(0) should dependOn(read0Output(0))
            mergeOutput(1) should dependOn(read0Output(1))
            mergeOutput(0) should dependOn(read1Output(0))
            mergeOutput(1) should dependOn(read1Output(1))
            mergeOutput(0) should dependOn(read2Output(0))
            mergeOutput(1) should dependOn(read2Output(1))

          }
        }
      }
    }
}
