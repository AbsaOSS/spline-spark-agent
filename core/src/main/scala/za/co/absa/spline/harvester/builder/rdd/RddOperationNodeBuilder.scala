/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.spline.harvester.builder.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.ExprId
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.builder.OperationNodeBuilder
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.IOAttributes
import za.co.absa.spline.producer.model.{Attribute, FunctionalExpression, Literal}

trait RddOperationNodeBuilder extends OperationNodeBuilder {

  def rdd: RDD[_]

  protected def idGenerators: IdGeneratorsBundle

  lazy val outputAttributes: IOAttributes = Seq.empty

  override def outputExprToAttMap: Map[ExprId, Attribute] = Map.empty

  lazy val functionalExpressions: Seq[FunctionalExpression] = Seq.empty

  lazy val literals: Seq[Literal] = Seq.empty

  protected def operationName: String = {
    Option(rdd.name).getOrElse(rdd.getClass.getSimpleName)
  }
}
