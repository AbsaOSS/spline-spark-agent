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

package za.co.absa.spline.harvester.builder.plan.read

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.ModelConstants.OperationExtras
import za.co.absa.spline.harvester.builder.plan.PlanOperationNodeBuilder
import za.co.absa.spline.harvester.builder.read.ReadCommand
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter, IOParamsConverter}
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.ReadOperation

class ReadNodeBuilder
  (val command: ReadCommand, val logicalPlan: LogicalPlan)
  (val idGenerators: IdGeneratorsBundle, val dataTypeConverter: DataTypeConverter, val dataConverter: DataConverter, postProcessor: PostProcessor)
  extends PlanOperationNodeBuilder {

  override protected type R = ReadOperation

  protected lazy val ioParamsConverter = new IOParamsConverter(exprToRefConverter)

  override def build(): ReadOperation = {
    val rop = ReadOperation(
      inputSources = command.sourceIdentifier.uris,
      id = operationId,
      name = logicalPlan.nodeName,
      output = outputAttributes.map(_.id),
      params = ioParamsConverter.convert(command.params),
      extra = command.extras ++ Map(OperationExtras.SourceType -> command.sourceIdentifier.format)
    )

    postProcessor.process(rop)
  }
}
