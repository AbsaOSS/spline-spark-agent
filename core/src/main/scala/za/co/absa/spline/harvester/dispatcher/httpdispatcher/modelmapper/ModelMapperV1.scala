/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher.httpdispatcher.modelmapper

import za.co.absa.spline.producer.model.v1_0
import za.co.absa.spline.producer.model.v1_1

import scala.collection.mutable


object ModelMapperV1 extends ModelMapper {

  // TODO

  /**
   * Convert ExecutionPlan v1.1 to ExecutionPlan v1.0
   */
  override def toDTO(plan: v1_1.ExecutionPlan): AnyRef = {
    val converter = new PlanConverter()

    v1_0.ExecutionPlan(
      plan.id,
      converter.toV1Operations(plan.operations),
      converter.toV1SystemInfo(plan.systemInfo),
      plan.agentInfo.map(converter.toV1AgentInfo(_)),
      plan.extraInfo
    )
  }

  private class PlanConverter {

    private var lastId = -1
    private val idMap: mutable.Map[String, Int] = mutable.Map()

    def toV1Operations(operations: v1_1.Operations) =
      v1_0.Operations(
        toV1WriteOperation(operations.write),
        operations.reads.map(ops => ops.map(toV1ReadOperation(_))),
        operations.other.map(ops => ops.map(toV1DataOperation(_)))
      )

    private def toV1WriteOperation(operation: v1_1.WriteOperation) =
      v1_0.WriteOperation(
        operation.outputSource,
        None, // schema ?
        operation.append,
        toV1Id(operation.id),
        operation.childIds.map(toV1Id(_)),
        operation.params,
        operation.extra
      )

    private def toV1ReadOperation(operation: v1_1.ReadOperation) =
      v1_0.ReadOperation(
        operation.childIds.map(toV1Id(_)),
        operation.inputSources,
        toV1Id(operation.id),
        None, // schema ?
        operation.params,
        operation.extra
      )

    private def toV1DataOperation(operation: v1_1.DataOperation) =
      v1_0.DataOperation(
        toV1Id(operation.id),
        operation.childIds.map(ops => ops.map(toV1Id(_))),
        None, // schema ?
        operation.params,
        operation.extra
      )

    private def toV1Id(idString: String) = {
      idMap.getOrElseUpdate(idString, getAndIncrementLastId())
    }

    private def getAndIncrementLastId() = {
      lastId = lastId + 1
      lastId
    }

    def toV1SystemInfo(nameAndVersion: v1_1.NameAndVersion) =
      v1_0.SystemInfo(nameAndVersion.name, nameAndVersion.version)

    def toV1AgentInfo(nameAndVersion: v1_1.NameAndVersion) =
      v1_0.AgentInfo(nameAndVersion.name, nameAndVersion.version)

  }


  /**
   * Convert ExecutionEvent v1.1 to ExecutionEvent v1.0
   */
  override def toDTO(event: v1_1.ExecutionEvent): AnyRef =
    v1_0.ExecutionEvent(
      event.planId,
      event.timestamp,
      event.error,
      event.extra
    )
}
