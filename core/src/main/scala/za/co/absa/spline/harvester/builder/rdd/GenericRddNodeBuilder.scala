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
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.DataOperation

class GenericRddNodeBuilder
  (val rdd: RDD[_])
  (val idGenerators: IdGeneratorsBundle, postProcessor: PostProcessor)
  extends RddOperationNodeBuilder {

  override protected type R = DataOperation

  override def build(): DataOperation = {
    val dop = DataOperation(
      id = operationId,
      name = operationName,
      childIds = childIds,
      output = outputAttributes.map(_.id),
      params = Map.empty,
      extra = Map.empty
    )

    postProcessor.process(dop)
  }

}
