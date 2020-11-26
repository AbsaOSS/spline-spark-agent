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

import org.apache.spark.sql.catalyst.expressions.{ExprId, Expression, Attribute => SparkAttribute}
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.OperationId
import za.co.absa.spline.harvester.converter.InputAttributeConverter.toSplineId
import za.co.absa.spline.producer.model.v1_1.Attribute

import scala.collection.mutable

class InputAttributeConverter(dataTypeConverter: DataTypeConverter) {

  private val store = mutable.Buffer[Attribute]()

  def values: Seq[Attribute] = store

  def convert(sparkExpr: Expression, operationId: String): Attribute = {
    val output = convertInternal(sparkExpr, operationId)
    store += output
    output
  }

  def convertInternal(expr: Expression, operationId: OperationId): Attribute = expr match {
    case attr: SparkAttribute =>
      Attribute(
        id = toSplineId(attr.exprId),
        dataType =  Some(dataTypeConverter.convert(attr.dataType, attr.nullable).id),
        childIds = List.empty,
        extra = ExpressionConverter.createExtra(attr, "expr.Attribute", operationId),
        name = attr.name
      )
  }
}

object InputAttributeConverter {

  def toSplineId(exprId: ExprId): String = {
    s"${exprId.jvmId}:${exprId.id}"
  }
}



