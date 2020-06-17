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

package za.co.absa.spline.harvester.builder.read

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.spline.harvester.ModelConstants.OperationExtras
import za.co.absa.spline.harvester.builder.OperationNodeBuilder
import za.co.absa.spline.harvester.extra.UserExtraMetadataProvider
import za.co.absa.spline.harvester.{ComponentCreatorFactory, HarvestingContext}
import za.co.absa.spline.producer.model.v1_1.ReadOperation

class ReadNodeBuilder
  (val command: ReadCommand)
  (val componentCreatorFactory: ComponentCreatorFactory, userExtraMetadataProvider: UserExtraMetadataProvider, ctx: HarvestingContext)
  extends OperationNodeBuilder {

  import za.co.absa.spline.harvester.ExtraMetadataImplicits._

  override protected type R = ReadOperation
  override val operation: LogicalPlan = command.operation

  override def build(): ReadOperation = {
    val rop = ReadOperation(
      childIds = Nil,
      inputSources = command.sourceIdentifier.uris.toList,
      id = id.toString,
      params = Map(command.params.toSeq: _*).asOption,
      extra = Map(
        OperationExtras.Name -> operation.nodeName,
        OperationExtras.SourceType -> command.sourceIdentifier.format
      ).asOption)

    rop.withAddedExtra(userExtraMetadataProvider.forOperation(rop, ctx))
  }
}
