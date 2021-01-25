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

package za.co.absa.spline.harvester.builder

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, LogicalPlan, Project, Union, Window}
import za.co.absa.spline.harvester.ComponentCreatorFactory
import za.co.absa.spline.harvester.builder.read.{ReadCommand, ReadNodeBuilder}
import za.co.absa.spline.harvester.builder.write.{WriteCommand, WriteNodeBuilder}
import za.co.absa.spline.harvester.postprocessing.PostProcessor

class OperationNodeBuilderFactory(
  postProcessor: PostProcessor,
  componentCreatorFactory: ComponentCreatorFactory
) {
  def writeNodeBuilder(wc: WriteCommand): WriteNodeBuilder =
    new WriteNodeBuilder(wc)(componentCreatorFactory, postProcessor)

  def readNodeBuilder(rc: ReadCommand): ReadNodeBuilder =
    new ReadNodeBuilder(rc)(componentCreatorFactory, postProcessor)

  def genericNodeBuilder(lp: LogicalPlan): OperationNodeBuilder = lp match {
    case p: Project => new ProjectNodeBuilder(p)(componentCreatorFactory, postProcessor)
    case u: Union => new UnionNodeBuilder(u)(componentCreatorFactory, postProcessor)
    case a: Aggregate => new AggregateNodeBuilder(a)(componentCreatorFactory, postProcessor)
    case g: Generate => new GenerateNodeBuilder(g)(componentCreatorFactory, postProcessor)
    case w: Window => new WindowNodeBuilder(w)(componentCreatorFactory, postProcessor)
    case _ => new GenericNodeBuilder(lp)(componentCreatorFactory, postProcessor)
  }
}
