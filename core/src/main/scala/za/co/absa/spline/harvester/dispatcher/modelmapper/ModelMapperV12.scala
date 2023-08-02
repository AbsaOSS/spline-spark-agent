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

import za.co.absa.spline.commons.SplineTraversableExtension._
import za.co.absa.spline.producer.dto.v1_2
import za.co.absa.spline.producer.model._

import scala.language.implicitConversions

object ModelMapperV12 extends ModelMapper[v1_2.ExecutionPlan, v1_2.ExecutionEvent] {

  override def toDTO(plan: ExecutionPlan): Option[v1_2.ExecutionPlan] = Some(v1_2.ExecutionPlan(
    id = plan.id,
    name = Some(plan.name),
    discriminator = plan.discriminator,
    labels = plan.labels.asNonEmptyOption,
    operations = toOperations(plan.operations),
    attributes = plan.attributes.map(toAttribute).asNonEmptyOption,
    expressions = Some(toExpressions(plan.expressions)),
    systemInfo = toNameAndVersion(plan.systemInfo),
    agentInfo = Some(toNameAndVersion(plan.agentInfo)),
    extraInfo = toUntypedMap(plan.extraInfo).asNonEmptyOption
  ))

  def toOperations(operations: Operations): v1_2.Operations = v1_2.Operations(
    write = toWriteOperation(operations.write),
    reads = operations.reads.map(toReadOperation).asNonEmptyOption,
    other = operations.other.map(toDataOperation).asNonEmptyOption
  )

  def toWriteOperation(operation: WriteOperation): v1_2.WriteOperation = v1_2.WriteOperation(
    outputSource = operation.outputSource,
    append = operation.append,
    id = operation.id,
    name = Some(operation.name),
    childIds = operation.childIds,
    params = toUntypedMap(operation.params).asNonEmptyOption,
    extra = toUntypedMap(operation.extra).asNonEmptyOption
  )

  def toReadOperation(operation: ReadOperation): v1_2.ReadOperation = v1_2.ReadOperation(
    inputSources = operation.inputSources,
    id = operation.id,
    name = Some(operation.name),
    output = operation.output.asNonEmptyOption,
    params = toUntypedMap(operation.params).asNonEmptyOption,
    extra = toUntypedMap(operation.extra).asNonEmptyOption
  )

  def toDataOperation(operation: DataOperation): v1_2.DataOperation = v1_2.DataOperation(
    id = operation.id,
    name = Some(operation.name),
    childIds = operation.childIds.asNonEmptyOption,
    output = operation.output.asNonEmptyOption,
    params = toUntypedMap(operation.params).asNonEmptyOption,
    extra = toUntypedMap(operation.extra).asNonEmptyOption
  )

  def toAttribute(attribute: Attribute): v1_2.Attribute = v1_2.Attribute(
    id = attribute.id,
    dataType = attribute.dataType,
    childRefs = attribute.childRefs.map(toAttrOrExprRef).asNonEmptyOption,
    extra = toUntypedMap(attribute.extra).asNonEmptyOption,
    name = attribute.name
  )

  def toAttrOrExprRef(o: AttrOrExprRef): v1_2.AttrOrExprRef = o match {
    case AttrRef(attrId) => v1_2.AttrOrExprRef(Some(attrId), None)
    case ExprRef(exprId) => v1_2.AttrOrExprRef(None, Some(exprId))
  }

  def toUntypedMap(map: Map[String, Any]): Map[String, Any] =
    ModelMapper.toUntypedMap(toAttrOrExprRef, map)

  def toExpressions(expressions: Expressions): v1_2.Expressions = v1_2.Expressions(
    functions = expressions.functions.map(toFunctionalExpression).asNonEmptyOption,
    constants = expressions.constants.map(toConstant).asNonEmptyOption
  )

  def toFunctionalExpression(fe: FunctionalExpression): v1_2.FunctionalExpression = v1_2.FunctionalExpression(
    id = fe.id,
    dataType = fe.dataType,
    childRefs = fe.childRefs.map(toAttrOrExprRef).asNonEmptyOption,
    extra = toUntypedMap(fe.extra).asNonEmptyOption,
    name = fe.name,
    params = toUntypedMap(fe.params).asNonEmptyOption
  )

  def toConstant(l: Literal): v1_2.Literal = v1_2.Literal(
    id = l.id,
    dataType = l.dataType,
    extra = toUntypedMap(l.extra).asNonEmptyOption,
    value = l.value
  )

  def toNameAndVersion(nav: NameAndVersion): v1_2.NameAndVersion = v1_2.NameAndVersion(
    name = nav.name,
    version = nav.version
  )

  override def toDTO(event: ExecutionEvent): Option[v1_2.ExecutionEvent] = Some(v1_2.ExecutionEvent(
    planId = event.planId,
    labels = event.labels.asNonEmptyOption,
    timestamp = event.timestamp,
    durationNs = event.durationNs,
    discriminator = event.discriminator,
    error = event.error,
    extra = toUntypedMap(event.extra).asNonEmptyOption
  ))

}
