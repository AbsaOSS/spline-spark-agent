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

package za.co.absa.spline.harvester.postprocessing

import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.producer.model.{DataOperation, ExecutionPlan}

class OneRowRelationFilter extends AbstractInternalPostProcessingFilter {

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = {
    val oneRowRelations = plan.operations.other
      .filter(_.name.startsWith("OneRowRelation"))

    if (oneRowRelations.isEmpty)
      plan
    else
      removeOneRowRelations(plan, oneRowRelations)
  }

  private def removeOneRowRelations(plan: ExecutionPlan, oneRowRelations: Seq[DataOperation]): ExecutionPlan = {
    val relIdSet = oneRowRelations.map(_.id).toSet

    val filteredOps = plan.operations.other.flatMap {
      case op if relIdSet(op.id) =>
        None

      case op if op.childIds.exists(relIdSet) =>
        val newChildIds = op.childIds.filterNot(relIdSet)
        Some(op.copy(childIds = newChildIds))

      case op =>
        Some(op)
    }

    plan.copy(
      operations = plan.operations.copy(
        other = filteredOps
      )
    )
  }

}
