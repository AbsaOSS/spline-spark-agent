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

package za.co.absa.spline.harvester.builder

import org.apache.spark.sql.catalyst.{expressions => sparkExprssions}
import org.apache.spark.sql.catalyst.plans.logical.Union
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.spline.harvester.ComponentCreatorFactory
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.v1_1.{AttrOrExprRef, Attribute, FunctionalExpression}

import java.util.UUID

class UnionNodeBuilder
  (override val operation: Union)
  (override val componentCreatorFactory: ComponentCreatorFactory, postProcessor: PostProcessor)
  extends GenericNodeBuilder(operation)(componentCreatorFactory, postProcessor) {

  private lazy val unionInputs: Seq[Seq[Attribute]] = inputAttributes.transpose

  override lazy val functionalExpressions: Seq[FunctionalExpression] =
    unionInputs
      .zip(operation.output)
      .map{ case (input, output) => constructUnionFunction(input, output)}

  override lazy val outputAttributes: Seq[Attribute] =
    unionInputs
      .zip(functionalExpressions)
      .map{ case (input, function) => constructUnionAttribute(input, function)}

  private def constructUnionFunction(
    inputSplineAttributes: Seq[Attribute],
    outputSparkAttribute: sparkExprssions.Attribute
  ) =
    FunctionalExpression(
      id =  UUID.randomUUID().toString,
      dataType = componentCreatorFactory.dataTypeConverter
        .convert(outputSparkAttribute.dataType, outputSparkAttribute.nullable).id.asOption,
      childIds = inputSplineAttributes.map(att => AttrOrExprRef(Some(att.id), None)).asOption,
      extra = Map("synthetic" -> "true").asOption,
      name = "union",
      params = Map("name" -> "union").asOption
    )

  private def constructUnionAttribute(inputSplineAttributes: Seq[Attribute], function: FunctionalExpression) =
    Attribute(
      id = UUID.randomUUID().toString,
      dataType = function.dataType,
      childIds = List(AttrOrExprRef(None, Some(function.id))).asOption,
      extra = Map("synthetic" -> "true").asOption,
      name = s"union of ${inputSplineAttributes.map(_.name).mkString(", ")}"
    )

}
