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
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.IOAttributes
import za.co.absa.spline.harvester.builder.plan.PlanOperationNodeBuilder.OperationId
import za.co.absa.spline.producer.model.{Attribute, FunctionalExpression, Literal}

trait OperationNodeBuilder {

  protected type R

  val operationId: OperationId = idGenerators.operationIdGenerator.nextId()

  protected var childBuilders: Seq[OperationNodeBuilder] = Nil

  def build(): R
  def addChild(childBuilder: OperationNodeBuilder): Unit = childBuilders :+= childBuilder
  protected def resolveAttributeChild(attribute: sparkExprssions.Attribute): Option[sparkExprssions.Expression] = None

  protected def inputAttributes: Seq[IOAttributes] = childBuilders.map(_.outputAttributes)
  protected def idGenerators: IdGeneratorsBundle

  def outputAttributes: IOAttributes

  def childIds: Seq[OperationId] = childBuilders.map(_.operationId)

  def functionalExpressions: Seq[FunctionalExpression]

  def literals: Seq[Literal]

  def outputExprToAttMap: Map[sparkExprssions.ExprId, Attribute]
}

object OperationNodeBuilder {
  type IOAttributes = Seq[Attribute]
}
