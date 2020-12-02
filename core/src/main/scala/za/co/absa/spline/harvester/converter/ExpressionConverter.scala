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
import za.co.absa.commons.lang.Converter
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.OperationId
import za.co.absa.spline.harvester.converter.ExpressionConverter._

trait ExpressionConverter extends Converter {

  override type From = (expressions.Expression, String)
  override type To = ExpressionLike
}

object ExpressionConverter {

  type ExpressionLike = {
    def id: String
  }

  /**
   * Serves as an expression placeholder for cases where there is no actual expression just id/reference of such.
   */
  case class TempReference(id: String)

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
