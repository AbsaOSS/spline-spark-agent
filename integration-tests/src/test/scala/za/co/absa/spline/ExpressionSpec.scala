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
import za.co.absa.commons.lang.OptionImplicits._
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

          assertStructuralEquivalence(plan)(
            Seq(
              attribute("_1", Seq.empty),
              attribute("_2", Seq.empty)),
            Seq(
              function("f-sum", Seq("_1", "_2"))),
            Seq.empty,
            Seq(
              attribute("sum", Seq("f-sum")))
          )
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

          assertStructuralEquivalence(plan)(
            Seq(
              attribute("_1", Seq.empty),
              attribute("_2", Seq.empty),
              attribute("sum", Seq("f-sum")),
              attribute("dif", Seq("f-dif")),
              attribute("mul", Seq("f-mul"))),
            Seq(
              function("f-sum", Seq("_1", "_2")),
              function("f-dif", Seq("_1", "_2")),
              function("f-mul", Seq("sum", "dif")),
              function("f-res", Seq("mul", "l-42"))),
            Seq(
              literal("l-42", 42)),
            Seq(
              attribute("res", Seq("f-res")))
          )
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

          assertStructuralEquivalence(plan)(
            Seq(
              attribute("_2", Seq.empty)),
            Seq(
              function("f-max", Seq("_2")),
              function("f-aggExp", Seq("f-max"))),
            Seq.empty,
            Seq(
              attribute("_1", Seq.empty),
              attribute("max2", Seq("f-aggExp")))
          )
        }
      })

  it should "convert union" in
    withNewSparkSession(spark =>
      withLineageTracking(spark) {
        lineageCaptor => {
          import spark.implicits._

          val df1 = Seq((1, 2)).toDF()
          val df2 = Seq((3, 4)).toDF()

          val unionizedDf = df1.union(df2)

          val (plan, _) = lineageCaptor.lineageOf(unionizedDf.write.mode("overwrite").save(filePath))

          assertStructuralEquivalence(plan)(
            Seq(
              attribute("_1", Seq.empty),
              attribute("_2", Seq.empty),
              attribute("_3", Seq.empty),
              attribute("_4", Seq.empty)),
            Seq(
              function("f-union-13", Seq("_1", "_3")),
              function("f-union-24", Seq("_2", "_4"))),
            Seq.empty,
            Seq(
              attribute("union-13", Seq("f-union-13")),
              attribute("union-24", Seq("f-union-24")))
          )
        }
      })
}

object ExpressionSpec extends Matchers {

  type Expr = Any // Literal | Attribute | FunctionalExpression

  def attribute(name: String, childIds: Seq[String]): Attribute =
    Attribute(name, None, childIds.map(idToAttrOrExprRef).asOption, None, name)

  def function(name: String, childIds: Seq[String]): FunctionalExpression =
    FunctionalExpression(name, None, childIds.map(idToAttrOrExprRef).asOption, None, name, None)

  def literal(name: String, value: Any): Literal =
    Literal(name, None, None, value)

  private def idToAttrOrExprRef(id: String) = id match {
    case i: String if i.startsWith("f-") => AttrOrExprRef(None, Some(i))
    case i: String if i.startsWith("l-") => AttrOrExprRef(None, Some(i))
    case i: String => AttrOrExprRef(Some(i), None)
  }

  def assertStructuralEquivalence(
    actualPlan: ExecutionPlan
  )(
    expectedAttributes: Seq[Attribute],
    expectedFunctions: Seq[FunctionalExpression],
    expectedLiterals: Seq[Literal],
    expectedOutput: Seq[Attribute]
  ): Unit = {
    val actualAttributes: Seq[Attribute] = actualPlan.attributes.get
    val actualFunctions: Seq[FunctionalExpression] = actualPlan.expressions.flatMap(_.functions).getOrElse(Seq.empty)
    val actualLiterals: Seq[Literal] = actualPlan.expressions.flatMap(_.constants).getOrElse(Seq.empty)

    actualAttributes.size shouldBe expectedAttributes.size + expectedOutput.size
    actualFunctions.size shouldBe actualFunctions.size
    actualLiterals.size shouldBe actualLiterals.size

    val actualIdMap = new IdMap(actualAttributes, actualFunctions, actualLiterals)
    val expectedIdMap = new IdMap(expectedAttributes ++ expectedOutput, expectedFunctions, expectedLiterals)

    val actualOutput: Seq[Attribute] = actualPlan.operations.write.output.map(actualIdMap.getAttribute)

    actualOutput.size shouldBe expectedOutput.size

    @scala.annotation.tailrec
    def assertDAGsIsomorphism(
      processedEntries: Set[(Expr, Expr)],
      stackedEntries: Seq[(Expr, Expr)]
    ): Set[(Expr, Expr)] = {
      stackedEntries match {
        case Nil => processedEntries

        case (actual, expected) +: restEnqueuedEntries if processedEntries((actual, expected)) =>
          assertDAGsIsomorphism(processedEntries, restEnqueuedEntries)

        case (actual, expected) +: restEnqueuedEntries =>
          assertSemiEquivalence(actual, expected)
          val children = getChildrenPairs(actual, expected)
          assertDAGsIsomorphism(
            processedEntries + stackedEntries.head,
            children ++ restEnqueuedEntries)
      }
    }

    actualOutput.zip(expectedOutput).foldLeft(Set.empty[(Expr, Expr)]){
      (processedPairs, pair) => assertDAGsIsomorphism(processedPairs, Seq(pair))
    }

    def assertSemiEquivalence(actual: Expr, expected: Expr): Unit = (actual, expected) match {
      case (al: Literal, el: Literal) =>
        al.value shouldBe el.value
      case (aa: Attribute, ea: Attribute) =>
        aa.childIds.map(_.size).getOrElse(0) shouldBe ea.childIds.map(_.size).getOrElse(0)
      case (af: FunctionalExpression, ef: FunctionalExpression) =>
        af.childIds.map(_.size).getOrElse(0) shouldBe ef.childIds.map(_.size).getOrElse(0)
    }

    def getChildrenPairs(actual: Expr, expected: Expr): Seq[(Expr, Expr)] = (actual, expected) match {
      case (_: Literal, _: Literal) =>
        Seq.empty
      case (aa: Attribute, ea: Attribute) =>
        zipChildren(aa.childIds, ea.childIds)
      case (af: FunctionalExpression, ef: FunctionalExpression) =>
        zipChildren(af.childIds, ef.childIds)
    }

    def zipChildren(
      actual: Option[Seq[AttrOrExprRef]],
      expected: Option[Seq[AttrOrExprRef]]
    ) = (actual, expected) match {
      case (None, None) => Seq.empty
      case (Some(al), Some(el)) => al.map(actualIdMap(_)).zip(el.map(expectedIdMap(_)))
    }
  }

  private class IdMap(attributes: Seq[Attribute], functions: Seq[FunctionalExpression], literals: Seq[Literal]) {
    val attMap = attributes.map(a => a.id -> a).toMap
    val expMap = functions.map(f => f.id -> f).toMap ++ literals.map(l => l.id -> l).toMap

    def getAttribute(id: String): Attribute = attMap(id)

    def apply(AOrERef: AttrOrExprRef): Expr = AOrERef match {
      case AttrOrExprRef(Some(attrId), None) => attMap(attrId)
      case AttrOrExprRef(None, Some(exprId)) => expMap(exprId)
    }
  }
}
