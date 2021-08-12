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

package za.co.absa.spline.harvester.builder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.Join
import za.co.absa.spline.harvester.ComponentCreatorFactory
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.v1_1.DataOperation

class JoinNodeBuilder
  (override val operation: Join)
  (override val componentCreatorFactory: ComponentCreatorFactory, postProcessor: PostProcessor)
  extends GenericNodeBuilder(operation)(componentCreatorFactory, postProcessor) with Logging {

  override def build(): DataOperation = {

    val duplicates = operation.output.groupBy(_.exprId).collect { case (x, List(_,_,_*)) => x }
    if (duplicates.nonEmpty) {
      logError(s"Duplicated attributes found in Join operation output, ExprIds of duplicates: $duplicates")
    }

    super.build()
  }

}
