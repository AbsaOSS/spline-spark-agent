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

package za.co.absa.spline.harvester.builder.plan

import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.{expressions => sparkExprssions}
import za.co.absa.spline.commons.lang.extensions.NonOptionExtension._
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.ModelConstants.CommonExtras
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.IOAttributes
import za.co.absa.spline.harvester.builder.plan.UnionNodeBuilder.Names
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.{AttrRef, Attribute, ExprRef, FunctionalExpression}

class UnionNodeBuilder
  (logicalPlan: Union)
  (idGenerators: IdGeneratorsBundle, dataTypeConverter: DataTypeConverter, dataConverter: DataConverter, postProcessor: PostProcessor)
  extends GenericPlanNodeBuilder(logicalPlan)(idGenerators, dataTypeConverter, dataConverter, postProcessor) {

  private lazy val unionInputs: Seq[IOAttributes] = inputAttributes.transpose

  override lazy val functionalExpressions: Seq[FunctionalExpression] =
    unionInputs
      .zip(logicalPlan.output)
      .map { case (input, output) => constructUnionFunction(input, output) }

  override lazy val outputAttributes: IOAttributes =
    unionInputs
      .zip(functionalExpressions)
      .map { case (input, function) => constructUnionAttribute(input, function) }

  private def constructUnionFunction(
    inputSplineAttributes: Seq[Attribute],
    outputSparkAttribute: sparkExprssions.Attribute
  ) =
    FunctionalExpression(
      id = idGenerators.expressionIdGenerator.nextId(),
      dataType = dataTypeConverter
        .convert(outputSparkAttribute.dataType, outputSparkAttribute.nullable).id.toOption,
      childRefs = inputSplineAttributes.map(att => AttrRef(att.id)),
      extra = Map(CommonExtras.Synthetic -> true),
      name = Names.Union,
      params = Map.empty
    )

  private def constructUnionAttribute(attributes: Seq[Attribute], function: FunctionalExpression) = {
    val attr1 = attributes.head
    Attribute(
      id = idGenerators.attributeIdGenerator.nextId(),
      dataType = function.dataType,
      childRefs = List(ExprRef(function.id)),
      extra = Map(CommonExtras.Synthetic -> true),
      name = attr1.name
    )
  }

}

object UnionNodeBuilder {

  object Names {
    val Union = "union"
  }

}
