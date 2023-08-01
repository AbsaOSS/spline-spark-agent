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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{ExternalRDD, LogicalRDD}
import za.co.absa.spline.commons.reflect.ReflectionUtils
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.LineageHarvester.{PlanOrRdd, PlanWrap, RddWrap}
import za.co.absa.spline.harvester.builder.OperationNodeBuilderFactory.AnalysisBarrierExtractor
import za.co.absa.spline.harvester.builder.plan._
import za.co.absa.spline.harvester.builder.plan.read.ReadNodeBuilder
import za.co.absa.spline.harvester.builder.plan.write.WriteNodeBuilder
import za.co.absa.spline.harvester.builder.rdd.GenericRddNodeBuilder
import za.co.absa.spline.harvester.builder.rdd.read.RddReadNodeBuilder
import za.co.absa.spline.harvester.builder.read.ReadCommand
import za.co.absa.spline.harvester.builder.write.WriteCommand
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.plugin.embedded.DeltaPlugin.{`_: MergeIntoCommandEdge`, `_: MergeIntoCommand`}
import za.co.absa.spline.harvester.postprocessing.PostProcessor

class OperationNodeBuilderFactory(
  postProcessor: PostProcessor,
  dataTypeConverter: DataTypeConverter,
  dataConverter: DataConverter,
  idGenerators: IdGeneratorsBundle
) {
  def writeNodeBuilder(wc: WriteCommand): WriteNodeBuilder =
    new WriteNodeBuilder(wc)(idGenerators, dataTypeConverter, dataConverter, postProcessor)

  def readNodeBuilder(rc: ReadCommand, planOrRdd: PlanOrRdd): OperationNodeBuilder = planOrRdd match {
    case PlanWrap(plan) => new ReadNodeBuilder(rc, plan)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case RddWrap(rdd) => new RddReadNodeBuilder(rc, rdd)(idGenerators, postProcessor)
  }

  def genericNodeBuilder(planOrRdd: PlanOrRdd): OperationNodeBuilder = planOrRdd match {
    case PlanWrap(plan) => genericPlanNodeBuilder(plan)
    case RddWrap(rdd) => genericRddNodeBuilder(rdd)
  }

  def nodeChildren(por: PlanOrRdd): Seq[PlanOrRdd] = por match {
    case PlanWrap(plan) =>
      plan match {
        case AnalysisBarrierExtractor(_) =>
          // special handling - spark 2.3 sometimes includes AnalysisBarrier in the plan
          val child = ReflectionUtils.extractValue[LogicalPlan](plan, "child")
          Seq(PlanWrap(child))
        case erdd: ExternalRDD[_] =>
          Seq(RddWrap(erdd.rdd))
        case lrdd: LogicalRDD =>
          Seq(RddWrap(lrdd.rdd))
        case `_: MergeIntoCommand`(command) =>
          MergeIntoNodeBuilder.extractChildren(command).map(PlanWrap)
        case `_: MergeIntoCommandEdge`(command) =>
          MergeIntoNodeBuilder.extractChildren(command).map(PlanWrap)
        case _ => plan.children.map(PlanWrap)
      }
    case RddWrap(rdd) =>
      rdd.dependencies.map(dep => RddWrap(dep.rdd))
  }

  private def genericPlanNodeBuilder(lp: LogicalPlan): OperationNodeBuilder = lp match {
    case p: Project => new ProjectNodeBuilder(p)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case u: Union => new UnionNodeBuilder(u)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case a: Aggregate => new AggregateNodeBuilder(a)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case g: Generate => new GenerateNodeBuilder(g)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case w: Window => new WindowNodeBuilder(w)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case j: Join => new JoinNodeBuilder(j)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case `_: MergeIntoCommand`(m) => new MergeIntoNodeBuilder(m)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case `_: MergeIntoCommandEdge`(m) => new MergeIntoNodeBuilder(m)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
    case _ => new GenericPlanNodeBuilder(lp)(idGenerators, dataTypeConverter, dataConverter, postProcessor)
  }

  private def genericRddNodeBuilder(rdd: RDD[_]): OperationNodeBuilder = rdd match {
    case _ => new GenericRddNodeBuilder(rdd)(idGenerators, postProcessor)
  }
}

object OperationNodeBuilderFactory {
  object AnalysisBarrierExtractor extends SafeTypeMatchingExtractor[LogicalPlan](
    "org.apache.spark.sql.catalyst.plans.logical.AnalysisBarrier")
}
