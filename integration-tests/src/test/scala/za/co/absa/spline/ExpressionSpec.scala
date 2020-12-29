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

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempFile
import za.co.absa.commons.lang.OptionImplicits.NonOptionWrapper
import za.co.absa.spline.producer.model.v1_1._
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture

class ExpressionSpec extends AnyFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture {

  import za.co.absa.spline.ExpressionSpec._

  private val filePath = TempFile("spline-expressions", ".parquet", pathOnly = false).deleteOnExit().path.toAbsolutePath.toString

  it should "convert sum of two columns" in
    withNewSparkSession(spark =>
      withLineageTracking(spark) {
        lineageCaptor => {
          import spark.implicits._

          val df = Seq((1, 2), (3, 4)).toDF()
            .select(col("_1") + col("_2") as "sum")

          val (plan, _) = lineageCaptor.lineageOf(df.write.mode("overwrite").save(filePath))

          assertStructuralEquivalence(
            plan.attributes.get,
            Seq(
              attribute("_1", Seq.empty),
              attribute("_2", Seq.empty),
              attribute("sum", Seq("f-sum"))))

          assertStructuralEquivalence(
            plan.expressions.get,
            Expressions(
              Some(Seq(
                function("f-sum", Seq("_1", "_2")))),
              None
            ))
        }
      })

  it should "convert ((a + b) * (a - b)) + 42" in
    withNewSparkSession(spark =>
      withLineageTracking(spark) {
        lineageCaptor => {
          import spark.implicits._

          val df = Seq((1, 2), (3, 4)).toDF()
            .select(col("_1") + col("_2") as "sum", col("_1") - col("_2") as "dif")
            .select(col("sum") * col("dif") as "mul")
            .select(col("mul") + 42 as "res")

          val (plan, _) = lineageCaptor.lineageOf(df.write.mode("overwrite").save(filePath))

          assertStructuralEquivalence(
            plan.attributes.get,
            Seq(
              attribute("_1", Seq.empty),
              attribute("_2", Seq.empty),
              attribute("sum", Seq("f-sum")),
              attribute("dif", Seq("f-dif")),
              attribute("mul", Seq("f-mul")),
              attribute("res", Seq("f-res"))))

          assertStructuralEquivalence(
            plan.expressions.get,
            Expressions(
              Some(Seq(
                function("f-sum", Seq("_1", "_2")),
                function("f-dif", Seq("_1", "_2")),
                function("f-mul", Seq("sum", "dif")),
                function("f-res", Seq("mul", "l-42")))),
              Some(Seq(
                literal("l-42", 42)))
            ))
        }
      })

  it should "convert aggregation" in
    withNewSparkSession(spark =>
      withLineageTracking(spark) {
        lineageCaptor => {
          import spark.implicits._

          val df = Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4)).toDF()
            .groupBy($"_1").agg(functions.max("_2") as "max2")

          val (plan, _) = lineageCaptor.lineageOf(df.write.mode("overwrite").save(filePath))

          assertStructuralEquivalence(
            plan.attributes.get,
            Seq(
              attribute("_1", Seq.empty),
              attribute("_2", Seq.empty),
              attribute("max2", Seq("f-aggExp"))))

          assertStructuralEquivalence(
            plan.expressions.get,
            Expressions(
              Some(Seq(
                function("f-max", Seq("_2")),
                function("f-aggExp", Seq("f-max")))),
              None
            ))
        }
      })
}

object ExpressionSpec extends Matchers {

  type Expr = Any // Literal | Attribute | FunctionalExpression

  def attribute(name: String, childIds: Seq[String]): Attribute =
    Attribute(name, None, childIds.map(x => AttrOrExprRef(Some(x), None)).asOption, None, name)

  def function(name: String, childIds: Seq[String]): FunctionalExpression =
    FunctionalExpression(name, None, childIds.map(x => AttrOrExprRef(None, Some(x))).asOption, None, name, None)

  def literal(name: String, value: Any): Literal =
    Literal(name, None, None, value)

  /**
   * for this to work expected attribute names must match the actual attribute names
   */
  def assertStructuralEquivalence(actual: Expressions, expected: Expressions): Unit = {
    actual.functions.size shouldBe expected.functions.size
    actual.constants.size shouldBe expected.constants.size
  }

  /**
   * for this to work expected attribute names must match the actual attribute names
   */
  def assertStructuralEquivalence(actual: Seq[Attribute], expected: Seq[Attribute]): Unit = {
    actual.size shouldBe expected.size

    val actualAttNameMap = actual.map(att => att.name -> att).toMap

    val attExpIdToActId = expected.map { expAtt =>
      val actualAtt = actualAttNameMap(expAtt.name)
      AttrOrExprRef(Some(expAtt.id), None) -> AttrOrExprRef(Some(actualAtt.id), None)
    }.toMap

    def toIdMap(attributes: Seq[Attribute]): Map[AttrOrExprRef, Attribute] = {
      attributes.map(a => AttrOrExprRef(Some(a.id), None) -> a).toMap
    }

    val actualIdMap = toIdMap(actual)
    val expectedIdMap = toIdMap(expected)

    expected.foreach { eAtt =>
      val aAtt = actualIdMap(attExpIdToActId(AttrOrExprRef(Some(eAtt.id), None)))
      val childrenPairs =
        aAtt.childIds.getOrElse(Nil)
          .zip(eAtt.childIds.getOrElse(Nil))
      childrenPairs.foreach {
        case (aId, eId) => actualIdMap(aId) shouldBe expectedIdMap(eId)
      }
    }
  }
}
