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

package za.co.absa.spline.harvester.dispatcher.modelmapper

import io.bfil.automapper._
import za.co.absa.spline.producer.dto.v1_2
import za.co.absa.spline.producer.model._

import scala.language.implicitConversions

/**
 * This class is using bfil automapper, unfortunately some parts of the mappings must be explicitly defined to
 * work around this bug: https://github.com/bfil/scala-automapper/issues/16
 */
object ModelMapperV12 extends ModelMapper[v1_2.ExecutionPlan, v1_2.ExecutionEvent] {

  import ImplicitMappingConversions._

  override def toDTO(plan: ExecutionPlan): Option[v1_2.ExecutionPlan] = Some(v1_2.ExecutionPlan(
    id = plan.id,
    name = plan.name,
    discriminator = plan.discriminator,
    labels = plan.labels,
    operations = toOperations(plan.operations),
    attributes = plan.attributes.map(automap(_).to[v1_2.Attribute]),
    expressions = Some(toExpressions(plan.expressions)),
    systemInfo = automap(plan.systemInfo).to[v1_2.NameAndVersion],
    agentInfo = Some(automap(plan.agentInfo).to[v1_2.NameAndVersion]),
    extraInfo = plan.extraInfo
  ))

  def toOperations(operations: Operations): v1_2.Operations = v1_2.Operations(
    write = automap(operations.write).to[v1_2.WriteOperation],
    reads = operations.reads.map(automap(_).to[v1_2.ReadOperation]),
    other = operations.other.map(automap(_).to[v1_2.DataOperation])
  )

  def toExpressions(expressions: Expressions): v1_2.Expressions = v1_2.Expressions(
    functions = expressions.functions.map(automap(_).to[v1_2.FunctionalExpression]),
    constants = expressions.constants.map(automap(_).to[v1_2.Literal])
  )

  override def toDTO(event: ExecutionEvent): Option[v1_2.ExecutionEvent] = Some(automap(event).to[v1_2.ExecutionEvent])

  implicit def toAttrOrExprRef(o: Seq[AttrOrExprRef]): Option[Seq[v1_2.AttrOrExprRef]] = o.map {
    case AttrRef(attrId) => v1_2.AttrOrExprRef(Some(attrId), None)
    case ExprRef(exprId) => v1_2.AttrOrExprRef(None, Some(exprId))
  }
}
