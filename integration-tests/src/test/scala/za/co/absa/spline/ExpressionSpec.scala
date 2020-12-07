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
import za.co.absa.spline.producer.model.v1_1.{Attribute, Expressions, FunctionalExpression, Literal}
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture

class ExpressionSpec extends AnyFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture {

  import za.co.absa.spline.ExpressionSpec._

  private val filePath = TempFile("spline-expressions", ".parquet", false).deleteOnExit().path.toAbsolutePath.toString

  it should "convert sum of two columns" in
    withNewSparkSession(spark =>
      withLineageTracking(spark) {
        lineageCaptor => {
          import spark.implicits._

          val df = Seq((1, 2), (3, 4)).toDF()
            .select(col("_1") + col("_2") as "sum")

          val (plan, _) = lineageCaptor.lineageOf(df.write.mode("overwrite").save(filePath))

          val expected = Expressions(
            List(
              attribute("_1", List.empty),
              attribute("_2", List.empty),
              attribute("sum", List("f-sum"))),
            List(
              function("f-sum", List("_1", "_2"))),
            List.empty
          )

          assertStructuralEquivalence(plan.expressions.get, expected)
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

          val expected = Expressions(
            List(
              attribute("_1", List.empty),
              attribute("_2", List.empty),
              attribute("sum", List("f-sum")),
              attribute("dif", List("f-dif")),
              attribute("mul", List("f-mul")),
              attribute("res", List("f-res"))),
            List(
              function("f-sum", List("_1", "_2")),
              function("f-dif", List("_1", "_2")),
              function("f-mul", List("sum", "dif")),
              function("f-res", List("mul", "l-42"))),
            List(
              literal("l-42", 42))
          )

          assertStructuralEquivalence(plan.expressions.get, expected)
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

          val expected = Expressions(
            List(
              attribute("_1", List.empty),
              attribute("_2", List.empty),
              attribute("max2", List("f-aggExp"))),
            List(
              function("f-max", List("_2")),
              function("f-aggExp", List("f-max"))),
            List.empty
          )

          assertStructuralEquivalence(plan.expressions.get, expected)
        }
      })
}

object ExpressionSpec extends Matchers {

  type Expr = Any // Literal | Attribute | FunctionalExpression

  def attribute(name: String, childIds: List[String]): Attribute =
    Attribute(name, None, childIds, Map.empty, name)

  def function(name: String, childIds: List[String]): FunctionalExpression =
    FunctionalExpression(name, None, childIds, Map.empty, name, Map.empty)

  def literal(name: String, value: Any): Literal =
    Literal(name, None, Map.empty, value)

  /**
   * for this to work expected attribute names must match the actual attribute names
   */
  def assertStructuralEquivalence(actual: Expressions, expected: Expressions): Unit = {
    actual.attributes.size shouldBe expected.attributes.size
    actual.functions.size shouldBe expected.functions.size
    actual.constants.size shouldBe expected.constants.size

    val actualAttNameMap = actual.attributes.map(att => att.name -> att).toMap

    val attExpIdToActId = expected.attributes.map { expAtt =>
      val actualAtt = actualAttNameMap(expAtt.name)
      expAtt.id -> actualAtt.id
    }.toMap

    def toIdMap(expressions: Expressions): Map[String, Expr] = {
      val attMap = expressions.attributes.map(a => a.id -> a).toMap
      val funMap = expressions.functions.map(f => f.id -> f).toMap
      val litMap = expressions.constants.map(l => l.id -> l).toMap
      attMap ++ funMap ++ litMap
    }

    val actualIdMap = toIdMap(actual)
    val expectedIdMap = toIdMap(expected)

    def compareReferences(actualExpr: Expr, expectedExpr: Expr): Unit = (actualExpr, expectedExpr) match {
      case (al: Literal, el: Literal) => al.value shouldBe el.value
      case (aa: Attribute, ea: Attribute) => aa.id shouldBe attExpIdToActId(ea.id)
      case (af: FunctionalExpression, ef: FunctionalExpression) =>
        val childrenPairs = af.childIds.zip(ef.childIds)
        childrenPairs.foreach {
          case (aId, eId) => compareReferences(actualIdMap(aId), expectedIdMap(eId))
        }
    }

    expected.attributes.foreach { eAtt =>
      val aAtt = actualIdMap(attExpIdToActId(eAtt.id)).asInstanceOf[Attribute]
      val childrenPairs = aAtt.childIds.zip(eAtt.childIds)
      childrenPairs.foreach {
        case (aId, eId) => compareReferences(actualIdMap(aId), expectedIdMap(eId))
      }
    }
  }
}
