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

package za.co.absa.spline.harvester.converter

import org.apache.commons.lang3.StringUtils.substringAfter
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.ExprId
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.OperationId
import za.co.absa.spline.harvester.converter.ExpressionConverter.ExpressionLike
import za.co.absa.spline.model.TempReference
import za.co.absa.spline.producer.model.v1_1.{Attribute, FunctionalExpression, Literal}

import scala.collection.mutable

trait ExpressionConverter {
  def convert(sparkExpr: expressions.Expression, operationId: String): ExpressionLike

  private val attributeStorage = mutable.Buffer[Attribute]()
  private val literalStorage = mutable.Buffer[Literal]()
  private val functionalExpressionStorage = mutable.Buffer[FunctionalExpression]()

  def attributes: List[Attribute] = attributeStorage.toList
  def literals: List[Literal] = literalStorage.toList
  def functionalExpressions: List[FunctionalExpression] = functionalExpressionStorage.toList

  protected def store(expr: ExpressionLike): Unit = expr match {
    case a: Attribute => attributeStorage += a
    case l: Literal => literalStorage += l
    case fe: FunctionalExpression => functionalExpressionStorage += fe
    case _: TempReference => // do nothing
  }
}

object ExpressionConverter {

  type ExpressionLike = {
    def id: String
  }

  def toSplineAttrId(exprId: ExprId): String = {
    s"${exprId.jvmId}:${exprId.id}"
  }

  private def getExpressionSimpleClassName(expr: expressions.Expression) = {
    val fullName = expr.getClass.getName
    val simpleName = substringAfter(fullName, "org.apache.spark.sql.catalyst.expressions.")
    if (simpleName.nonEmpty) simpleName else fullName
  }

  def createExtra(expr: expressions.Expression, typeHint: String, operationId: OperationId) = Map(
    "simpleClassName" -> getExpressionSimpleClassName(expr),
    "_typeHint" -> typeHint,
    "operationId" -> operationId
  )

}
