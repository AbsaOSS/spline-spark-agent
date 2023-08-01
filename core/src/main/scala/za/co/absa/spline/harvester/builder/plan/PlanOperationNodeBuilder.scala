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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{expressions => sparkExprssions}
import za.co.absa.spline.commons.lang.CachingConverter
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.builder.OperationNodeBuilder
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.IOAttributes
import za.co.absa.spline.harvester.converter._
import za.co.absa.spline.producer.model.{Attribute, FunctionalExpression, Literal}

trait PlanOperationNodeBuilder extends OperationNodeBuilder {

  def logicalPlan: LogicalPlan

  protected def idGenerators: IdGeneratorsBundle
  protected def dataTypeConverter: DataTypeConverter
  protected def dataConverter: DataConverter

  protected lazy val attributeConverter =
    new AttributeConverter(
      idGenerators.attributeIdGenerator,
      dataTypeConverter,
      resolveAttributeChild,
      childBuilders.map(_.outputExprToAttMap).reduceOption(_ ++ _).getOrElse(Map.empty),
      exprToRefConverter
    ) with CachingConverter

  protected lazy val expressionConverter =
    new ExpressionConverter(
      idGenerators.expressionIdGenerator,
      dataTypeConverter,
      exprToRefConverter
    ) with CachingConverter

  protected lazy val literalConverter =
    new LiteralConverter(
      idGenerators.expressionIdGenerator,
      dataConverter,
      dataTypeConverter
    ) with CachingConverter

  protected lazy val exprToRefConverter: ExprToRefConverter =
    new ExprToRefConverter(
      attributeConverter,
      expressionConverter,
      literalConverter
    )

  lazy val outputAttributes: IOAttributes =
    logicalPlan.output.map(attributeConverter.convert)

  override def outputExprToAttMap: Map[sparkExprssions.ExprId, Attribute] =
    logicalPlan.output.map(_.exprId).zip(outputAttributes).toMap

  lazy val functionalExpressions: Seq[FunctionalExpression] = expressionConverter.values

  lazy val literals: Seq[Literal] = literalConverter.values
}

object PlanOperationNodeBuilder {
  type OperationId = String
}
