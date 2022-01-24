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

package za.co.absa.spline.harvester

import za.co.absa.spline.harvester.IdGenerator._
import za.co.absa.spline.harvester.IdGenerators._
import za.co.absa.spline.producer.model.ExecutionPlan

import java.util.UUID

class IdGenerators(execPlanUUIDGeneratorFactory: UUIDGeneratorFactory[UUIDNamespace, ExecutionPlan]) {
  val execPlanIdGenerator: IdGenerator[ExecutionPlan, UUID] = execPlanUUIDGeneratorFactory(UUIDNamespace.ExecutionPlan)
  val attributeIdGenerator: IdGenerator[Any, String] = new SequentialIdGenerator(AttributeIdTemplate)
  val expressionIdGenerator: IdGenerator[Any, String] = new SequentialIdGenerator(ExpressionIdTemplate)
  val operationIdGenerator: IdGenerator[Any, String] = new SequentialIdGenerator(OperationIdTemplate)
  val dataTypeIdGenerator: IdGenerator[Any, UUID] =
    new ComposableIdGenerator(
      new SequentialIdGenerator("{0}"),
      new UUID5IdGenerator[String](UUIDNamespace.DataType)
    )
}

object IdGenerators {
  val AttributeIdTemplate = "attr-{0}"
  val ExpressionIdTemplate = "expr-{0}"
  val OperationIdTemplate = "op-{0}"
}
