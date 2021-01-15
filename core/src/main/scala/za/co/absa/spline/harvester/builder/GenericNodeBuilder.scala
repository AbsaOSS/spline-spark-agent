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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.spline.harvester.ComponentCreatorFactory
import za.co.absa.spline.harvester.ModelConstants.OperationExtras
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.v1_1.DataOperation

class GenericNodeBuilder
  (val operation: LogicalPlan)
  (val componentCreatorFactory: ComponentCreatorFactory, postProcessor: PostProcessor)
  extends OperationNodeBuilder {

  override protected type R = DataOperation

  override def build(): DataOperation = {

    enrich(operation)

    val dop = DataOperation(
      id = id,
      childIds = childIds.toList.asOption,
      output = outputAttributes,
      params = componentCreatorFactory.operationParamsConverter.convert(operation).asOption,
      extra = Map(OperationExtras.Name -> operation.nodeName).asOption
    )

    postProcessor.process(dop)
  }

  private def enrich(logicalPlan: LogicalPlan): Unit = logicalPlan match {
    case u: Union => componentCreatorFactory.unionCreator.create(u)
    case _ => Unit
  }
}
