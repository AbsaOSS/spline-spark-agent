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
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.spline.model.TempReference
import za.co.absa.spline.producer.model.v1_1._

import scala.:+


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
        id = AttributeConverter.toSplineId(a.exprId),
        dataType = Some(getDataType(a).id),
        childIds = List(convert(a.child).id),
        extra = Map("_typeHint" -> "expr.Attribute"), // this was Alias
        name = a.name
      )

    case a: expressions.AttributeReference =>
      TempReference(AttributeConverter.toSplineId(a.exprId))

    case lit: expressions.Literal =>
      Literal(
        id = UUID.randomUUID().toString,
        dataType = Some(getDataType(lit).id),
        extra = Map("_typeHint" -> "expr.Literal"),
        value = dataConverter.convert(Tuple2.apply(lit.value, lit.dataType))
      )
//      expr.Literal.apply(dataConverter.convert(Tuple2.apply(lit.value, lit.dataType)), getDataType(lit).id)

    case bo: expressions.BinaryOperator =>
      FunctionalExpression(
        id = UUID.randomUUID().toString,
        dataType = Some(getDataType(bo).id),
        childIds = bo.children.map(convert).map(_.id).toList,
        extra = Map("_typeHint" -> "expr.Binary"),
        name = bo.prettyName,
        params = getExpressionExtraParameters(bo) + ("symbol" -> bo.symbol)
      )
//      expr.Binary(
//        bo.symbol,
//        getDataType(bo).id,
//        bo.children map convert)
//

    case u: expressions.ScalaUDF =>
      FunctionalExpression(
        id = UUID.randomUUID().toString,
        dataType = Some(getDataType(u).id),
        childIds = u.children.map(convert).map(_.id).toList,
        extra = Map("_typeHint" -> "expr.UDF"),
        name = u.udfName getOrElse u.function.getClass.getName,
        params = getExpressionExtraParameters(u)
      )
//      expr.UDF(
//        u.udfName getOrElse u.function.getClass.getName,
//        getDataType(u).id,
//        u.children map convert)
//

    case e: expressions.LeafExpression =>
      FunctionalExpression(
        id = UUID.randomUUID().toString,
        dataType = Some(getDataType(e).id),
        childIds = List.empty,
        extra = Map("_typeHint" -> "expr.GenericLeaf"),
        name = e.prettyName,
        params = getExpressionExtraParameters(e) // params
      )
//      expr.GenericLeaf(
//        e.prettyName,
//        getDataType(e).id,
//        getExpressionSimpleClassName(e),
//        getExpressionExtraParameters(e))
//

    case _: expressions.WindowSpecDefinition | _: expressions.WindowFrame =>
      FunctionalExpression(
        id = UUID.randomUUID().toString,
        dataType = None,
        childIds = sparkExpr.children.map(convert).map(_.id).toList,
        extra = Map("_typeHint" -> "expr.UntypedExpression"),
        name = sparkExpr.prettyName,
        params = getExpressionExtraParameters(sparkExpr) // params
      )
//      expr.UntypedExpression(
//        sparkExpr.prettyName,
//        sparkExpr.children map convert,
//        getExpressionSimpleClassName(sparkExpr), // exprType
//        getExpressionExtraParameters(sparkExpr)) // params
//

    case e: expressions.Expression =>
      FunctionalExpression(
        id = UUID.randomUUID().toString,
        dataType = Some(getDataType(e).id),
        childIds = e.children.map(convert).map(_.id).toList,
        extra = Map("_typeHint" -> "expr.Generic"),
        name = sparkExpr.prettyName,
        params = getExpressionExtraParameters(sparkExpr) // params
      )

//      expr.Generic(
//        e.prettyName,
//        getDataType(e).id,
//        e.children map convert,
//        getExpressionSimpleClassName(e),
//        getExpressionExtraParameters(e))
  }

  private def getDataType(expr: SparkExpression) = dataTypeConverter.convert(expr.dataType, expr.nullable)
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

