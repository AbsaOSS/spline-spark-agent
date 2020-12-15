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

package za.co.absa.spline.harvester.extra

import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.harvester.postprocessing.LineageFilter
import za.co.absa.spline.producer.model.v1_1._
import za.co.absa.spline.harvester.ExtraMetadataImplicits._

/**
 * provides backward compatibility for older less powerful UserExtraMetadataProvider API
 */
class UserExtraAppendingLineageFilter(userExtraMetadataProvider: UserExtraMetadataProvider) extends LineageFilter {

  override def processExecutionEvent(event: ExecutionEvent, ctx: HarvestingContext): ExecutionEvent =
    event.withAddedExtra(userExtraMetadataProvider.forExecEvent(event, ctx))

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext ): ExecutionPlan =
    plan.withAddedExtra(userExtraMetadataProvider.forExecPlan(plan, ctx))

  override def processReadOperation(op: ReadOperation, ctx: HarvestingContext ): ReadOperation =
    op.withAddedExtra(userExtraMetadataProvider.forOperation(op, ctx))

  override def processWriteOperation(op: WriteOperation, ctx: HarvestingContext): WriteOperation =
    op.withAddedExtra(userExtraMetadataProvider.forOperation(op, ctx))

  override def processDataOperation(op: DataOperation, ctx: HarvestingContext  ): DataOperation =
    op.withAddedExtra(userExtraMetadataProvider.forOperation(op, ctx))
}
