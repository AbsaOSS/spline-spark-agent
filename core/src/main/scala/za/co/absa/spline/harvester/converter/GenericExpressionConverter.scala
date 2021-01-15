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

package za.co.absa.spline.harvester.converter

import java.util.UUID
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.spline.harvester.converter.ExpressionConverter._
import za.co.absa.spline.producer.model.v1_1._


class GenericExpressionConverter(
  dataConverter: DataConverter,
  dataTypeConverter: DataTypeConverter,
  attributeIdResolver: AttributeIdResolver
) extends  ExpressionConverter {

  import GenericExpressionConverter._

  override def convert(sparkExpr: SparkExpression): ExpressionLike = sparkExpr match {

    case a: expressions.Alias =>
      Attribute(
        id = attributeIdResolver.resolve(a.exprId),
        dataType = convertDataType(a),
        childIds = List(toAttrOrExprRef(convert(a.child))).asOption,
        extra = createExtra(a, "expr.Attribute"),
        name = a.name
      )

    case a: expressions.Attribute =>
      TempReference(attributeIdResolver.resolve(a.exprId))

    case lit: expressions.Literal =>
      Literal(
        id = randomUUIDString,
        dataType = convertDataType(lit),
        extra = createExtra(lit, "expr.Literal"),
        value = dataConverter.convert(Tuple2.apply(lit.value, lit.dataType))
      )

    case bo: expressions.BinaryOperator =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(bo),
        childIds = convertChildren(bo),
        extra = createExtra(bo, "expr.Binary"),
        name = bo.prettyName,
        params = getExpressionExtraParameters(bo)
          .orElse(Some(Map.empty[String, Any]))
          .map(_ + ("symbol" -> bo.symbol))
      )

    case u: expressions.ScalaUDF =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(u),
        childIds = convertChildren(u),
        extra = createExtra(u, "expr.UDF"),
        name = u.udfName getOrElse u.function.getClass.getName,
        params = getExpressionExtraParameters(u)
      )

    case e: expressions.LeafExpression =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(e),
        childIds = None,
        extra = createExtra(e, "expr.GenericLeaf"),
        name = e.prettyName,
        params = getExpressionExtraParameters(e)
      )

    case _: expressions.WindowSpecDefinition | _: expressions.WindowFrame =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = None,
        childIds = convertChildren(sparkExpr),
        extra = createExtra(sparkExpr, "expr.UntypedExpression"),
        name = sparkExpr.prettyName,
        params = getExpressionExtraParameters(sparkExpr)
      )

    case e: expressions.Expression =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(e),
        childIds = convertChildren(e),
        extra = createExtra(e, "expr.Generic"),
        name = e.prettyName,
        params = getExpressionExtraParameters(e)
      )
  }

  private def randomUUIDString = {
    UUID.randomUUID().toString
  }

  private def convertDataType(expr: SparkExpression) = {
    val dataType = dataTypeConverter.convert(expr.dataType, expr.nullable)
    Some(dataType.id)
  }

  private def convertChildren(e: SparkExpression) = {
    e.children
      .map(expr => convert(expr))
      .map(toAttrOrExprRef)
      .asOption
  }

  def toAttrOrExprRef(exprLike: ExpressionLike): AttrOrExprRef = exprLike match {
    case a: Attribute => AttrOrExprRef(Some(a.id), None)
    case ar: TempReference => AttrOrExprRef(Some(ar.id), None)
    case e: ExpressionLike => AttrOrExprRef(None, Some(e.id))
  }
}

object GenericExpressionConverter {

  private val basicProperties = Set("children", "dataType", "nullable")

  private def getExpressionExtraParameters(e: SparkExpression): Option[Map[String, Any]] = {
    val isChildExpression: Any => Boolean = {
      val children = e.children.toSet
      PartialFunction.cond(_) {
        case ei: SparkExpression if children(ei) => true
      }
    }

    val renderedParams =
      for {
        (p, v) <- ReflectionUtils.extractProperties(e)
        if !basicProperties(p)
        if !isChildExpression(v)
        w <- ValueDecomposer.decompose(v, Unit)
      } yield p -> w

    renderedParams.asOption
  }

}

