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

import org.apache.commons.lang3.StringUtils.substringAfter
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import za.co.absa.commons.lang.Converter
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.spline.model.TempReference
import za.co.absa.spline.producer.model.v1_1._


class ExpressionConverter(
  dataConverter: DataConverter,
  dataTypeConverter: DataTypeConverter)
  extends Converter {

  import ExpressionConverter._

  override type From = SparkExpression
  override type To = ExpressionLike

  override def convert(sparkExpr: SparkExpression): ExpressionLike = sparkExpr match {

    case a: expressions.Alias =>
      Attribute(
        id = InputAttributeConverter.toSplineId(a.exprId),
        dataType = convertDataType(a),
        childIds = List(convert(a.child).id),
        extra = Map("_typeHint" -> "expr.Attribute"),
        name = a.name
      )

    case a: expressions.AttributeReference =>
      TempReference(InputAttributeConverter.toSplineId(a.exprId))

    case lit: expressions.Literal =>
      Literal(
        id = randomUUIDString,
        dataType = convertDataType(lit),
        extra = Map("_typeHint" -> "expr.Literal"),
        value = dataConverter.convert(Tuple2.apply(lit.value, lit.dataType))
      )

    case bo: expressions.BinaryOperator =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(bo),
        childIds = convertChildren(bo),
        extra = Map("_typeHint" -> "expr.Binary"),
        name = getExpressionSimpleClassName(bo),
        params = getExpressionExtraParameters(bo) + ("symbol" -> bo.symbol)
      )

    case u: expressions.ScalaUDF =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(u),
        childIds = convertChildren(u),
        extra = Map("_typeHint" -> "expr.UDF"),
        name = u.udfName getOrElse u.function.getClass.getName,
        params = getExpressionExtraParameters(u)
      )

    case e: expressions.LeafExpression =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(e),
        childIds = List.empty,
        extra = Map("_typeHint" -> "expr.GenericLeaf"),
        name = getExpressionSimpleClassName(e),
        params = getExpressionExtraParameters(e)
      )

    case _: expressions.WindowSpecDefinition | _: expressions.WindowFrame =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = None,
        childIds = convertChildren(sparkExpr),
        extra = Map("_typeHint" -> "expr.UntypedExpression"),
        name = getExpressionSimpleClassName(sparkExpr),
        params = getExpressionExtraParameters(sparkExpr)
      )

    case e: expressions.Expression =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(e),
        childIds = convertChildren(e),
        extra = Map("_typeHint" -> "expr.Generic"),
        name = getExpressionSimpleClassName(e),
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
    e.children.map(convert).map(_.id).toList
  }

}

object ExpressionConverter {

  type ExpressionLike = {
    def id: String
  }

  private val basicProperties = Set("children", "dataType", "nullable")

  val foo = basicProperties.getClass()

  private def getExpressionSimpleClassName(expr: SparkExpression) = {
    val fullName = expr.getClass.getName
    val simpleName = substringAfter(fullName, "org.apache.spark.sql.catalyst.expressions.")
    if (simpleName.nonEmpty) simpleName else fullName
  }

  private def getExpressionExtraParameters(e: SparkExpression): Map[String, Any] = {
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

    renderedParams
  }
}

