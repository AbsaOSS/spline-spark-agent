/*
 * Copyright 2021 ABSA Group Limited
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

import org.apache.spark.sql.catalyst.expressions.{Attribute => SparkAttribute}
import org.apache.spark.sql.catalyst.plans.logical.Union
import za.co.absa.commons.lang.OptionImplicits.NonOptionWrapper
import za.co.absa.spline.producer.model.v1_1.{AttrOrExprRef, Attribute, FunctionalExpression, Literal}

import java.util.UUID

class UnionLineageCreator(dataTypeConverter: DataTypeConverter, attributeIdResolver: AttributeIdResolver){

  val expressionAccumulator = new ExpressionAccumulator

  def create(unionOp: Union): Any = {
    val sequenceOfUnionInputs = unionOp
      .children
      .map(_.output)
      .transpose

    val toStore = sequenceOfUnionInputs
      .zip(unionOp.output)
      .map{ case (inputIds, output) => constructUnionExpression(inputIds, output) }
      .map(_.apply()) // change id mapping only after all functions and attributes were created

    toStore.foreach {
      case (fe, a) =>
        expressionAccumulator.store(fe)
        expressionAccumulator.store(a)
    }
  }

  private def constructUnionExpression(inputSplineAttributes: Seq[SparkAttribute], outputSparkAttribute: SparkAttribute) = {
    val function = FunctionalExpression(
      id =  UUID.randomUUID().toString,
      dataType = dataTypeConverter.convert(outputSparkAttribute.dataType, outputSparkAttribute.nullable).id.asOption,
      childIds = inputSplineAttributes.map(toSplineAttrOrExprRef).asOption,
      extra = Map("synthetic" -> "true").asOption,
      name = "union",
      params = Map("name" -> "union").asOption
    )

    val attribute = Attribute(
      id = UUID.randomUUID().toString,
      dataType = function.dataType,
      childIds = List(AttrOrExprRef(None, Some(function.id))).asOption,
      extra = Map("synthetic" -> "true").asOption,
      name = "union of " + createUnionName(inputSplineAttributes)
    )

    () => {
      attributeIdResolver.updateMapping(outputSparkAttribute.exprId, attribute.id)
      (function, attribute)
    }
  }

  private def toSplineAttrOrExprRef(sparkAttribute: SparkAttribute) = {
    val resolvedId = attributeIdResolver.resolve(sparkAttribute.exprId)
    AttrOrExprRef(Some(resolvedId), None)
  }

  private def createUnionName(createUnionName: Seq[SparkAttribute]) = {
    createUnionName.map(_.name).mkString(", ")
  }

}

