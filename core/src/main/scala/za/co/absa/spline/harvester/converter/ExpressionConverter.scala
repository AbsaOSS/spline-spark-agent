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

import org.apache.commons.lang3.StringUtils.substringAfter
import org.apache.spark.sql.catalyst.{expressions => sparkExprssions}
import za.co.absa.commons.lang.Converter
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.spline.producer.model.v1_1._

import java.util.UUID
import scala.language.reflectiveCalls


class ExpressionConverter(
  dataTypeConverter: DataTypeConverter,
  exprTpRefConverter: => ExprToRefConverter
) extends Converter {

  import ExpressionConverter._
  import za.co.absa.commons.lang.OptionImplicits._

  override type From = sparkExprssions.Expression
  override type To = FunctionalExpression

  def convert(sparkExpr: From): To = sparkExpr match {

    case a: sparkExprssions.Alias =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(a),
        childIds = convertChildren(a).asOption,
        extra = createExtra(sparkExpr, "expr.Alias").asOption,
        name = a.name,
        params = getExpressionParameters(a).asOption
      )

    case bo: sparkExprssions.BinaryOperator =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(bo),
        childIds = convertChildren(bo).asOption,
        extra = createExtra(bo, "expr.Binary").asOption,
        name = bo.prettyName,
        params = (getExpressionParameters(bo) + ("symbol" -> bo.symbol)).asOption
      )

    case u: sparkExprssions.ScalaUDF =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(u),
        childIds = convertChildren(u).asOption,
        extra = createExtra(u, "expr.UDF").asOption,
        name = u.udfName getOrElse u.function.getClass.getName,
        params = getExpressionParameters(u).asOption
      )

    case e: sparkExprssions.LeafExpression =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(e),
        childIds = None,
        extra = createExtra(e, "expr.GenericLeaf").asOption,
        name = e.prettyName,
        params = getExpressionParameters(e).asOption
      )

    case _: sparkExprssions.WindowSpecDefinition
         | _: sparkExprssions.WindowFrame =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = None,
        childIds = convertChildren(sparkExpr).asOption,
        extra = createExtra(sparkExpr, "expr.UntypedExpression").asOption,
        name = sparkExpr.prettyName,
        params = getExpressionParameters(sparkExpr).asOption
      )

    case e: sparkExprssions.Expression =>
      FunctionalExpression(
        id = randomUUIDString,
        dataType = convertDataType(e),
        childIds = convertChildren(e).asOption,
        extra = createExtra(e, "expr.Generic").asOption,
        name = e.prettyName,
        params = getExpressionParameters(e).asOption
      )
  }

  private def randomUUIDString = {
    UUID.randomUUID().toString
  }

  private def convertDataType(expr: sparkExprssions.Expression) = {
    val dataType = dataTypeConverter.convert(expr.dataType, expr.nullable)
    Some(dataType.id)
  }

  private def convertChildren(e: sparkExprssions.Expression): Seq[AttrOrExprRef] = {
    e.children.map(exprTpRefConverter.convert)
  }

}

object ExpressionConverter {

  private val basicProperties = Set("children", "dataType", "nullable")

  private def getExpressionSimpleClassName(expr: sparkExprssions.Expression) = {
    val fullName = expr.getClass.getName
    val simpleName = substringAfter(fullName, "org.apache.spark.sql.catalyst.expressions.")
    if (simpleName.nonEmpty) simpleName else fullName
  }

  def createExtra(expr: sparkExprssions.Expression, typeHint: String) = Map(
    "simpleClassName" -> getExpressionSimpleClassName(expr),
    "_typeHint" -> typeHint
  )

  private def getExpressionParameters(e: sparkExprssions.Expression): Map[String, Any] = {
    val isChildExpression: Any => Boolean = {
      val children = e.children.toSet
      PartialFunction.cond(_) {
        case ei: sparkExprssions.Expression if children(ei) => true
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

