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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType
import za.co.absa.commons.lang.Converter
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.spline.harvester.converter.OperationParamsConverter._
import za.co.absa.spline.model.ExpressionReference

class OperationParamsConverter(
  dataConverter: DataConverter,
  expressionConverter: GenericExpressionConverter
) extends Converter {
  override type From = (LogicalPlan, String)
  override type To = Map[String, _]

  private def renderer(operationId: String) = ValueDecomposer.addHandler(_ => {
    case (row: InternalRow, rowType: DataType) => Some(dataConverter.convert((row, rowType)))
    case (jt: JoinType, _) => Some(jt.sql)
    case (so: SortOrder, _) => Some(Map(
      "expression" -> new ExpressionReference(expressionConverter.convert(so.child, operationId).id),
      "direction" -> so.direction.sql,
      "nullOrdering" -> so.nullOrdering.sql))
    case (exp: Expression, _) => Some(new ExpressionReference(expressionConverter.convert(exp, operationId).id))
  })

  override def convert(operationAndId: (LogicalPlan, String)): Map[String, _] = {
    val (operation, operationId) = operationAndId

    val isChildOperation: Any => Boolean = {
      val children = operation.children.toSet
      PartialFunction.cond(_) {
        case oi: LogicalPlan if children(oi) => true
      }
    }

    for {
      (p, v) <- ReflectionUtils.extractProperties(operation)
      if !KnownPropNames(p)
      if !IgnoredPropNames(p)
      if !isChildOperation(v)
    } yield
      p -> renderer(operationId).decompose(v, operation.schema)
  }
}

object OperationParamsConverter {
  private val KnownPropNames = Set("nodeName", "output", "children", "child")
  private val IgnoredPropNames = Set("data")
}
