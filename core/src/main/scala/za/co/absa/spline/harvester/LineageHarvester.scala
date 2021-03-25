/*
 * Copyright 2017 ABSA Group Limited
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

package za.co.absa.spline.harvester

import org.apache.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import scalaz.Scalaz._
import za.co.absa.commons.graph.GraphImplicits._
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.LineageHarvester._
import za.co.absa.spline.harvester.ModelConstants.{AppMetaInfo, ExecutionEventExtra, ExecutionPlanExtra}
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.builder.read.ReadCommandExtractor
import za.co.absa.spline.harvester.builder.write.WriteCommandExtractor
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode.SplineMode
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.logging.ObjectStructureDumper
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.v1_1._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class LineageHarvester(
  ctx: HarvestingContext,
  splineMode: SplineMode,
  writeCommandExtractor: WriteCommandExtractor,
  readCommandExtractor: ReadCommandExtractor,
  iwdStrategy: IgnoredWriteDetectionStrategy,
  postProcessor: PostProcessor
) extends Logging {

  private val componentCreatorFactory: ComponentCreatorFactory = new ComponentCreatorFactory
  private val opNodeBuilderFactory = new OperationNodeBuilderFactory(postProcessor, componentCreatorFactory)

  def harvest(result: Try[Duration]): HarvestResult = {
    logDebug(s"Harvesting lineage from ${ctx.logicalPlan.getClass}")

    val (readMetrics: Metrics, writeMetrics: Metrics) = ctx.executedPlanOpt.
      map(getExecutedReadWriteMetrics).
      getOrElse((Map.empty, Map.empty))

    val maybeCommand = Try(writeCommandExtractor.asWriteCommand(ctx.logicalPlan)) match {
      case Success(result) => result
      case Failure(e) => splineMode match {
        case SplineMode.REQUIRED =>
          throw e
        case SplineMode.BEST_EFFORT =>
          logWarning(e.getMessage)
          None
      }
    }


    if (maybeCommand.isEmpty) {
      logDebug(s"${ctx.logicalPlan.getClass} was not recognized as a write-command. Skipping.")
      logTrace(ObjectStructureDumper.dump(ctx.logicalPlan))
    }

    maybeCommand.flatMap(writeCommand => {
      val writeOpBuilder = opNodeBuilderFactory.writeNodeBuilder(writeCommand)
      val restOpBuilders = createOperationBuildersRecursively(writeCommand.query)

      restOpBuilders.lastOption.foreach(writeOpBuilder.+=)
      val builders = restOpBuilders :+ writeOpBuilder

      val restOps = restOpBuilders.map(_.build())
      val writeOp = writeOpBuilder.build()

      val (opReads, opOthers) =
        ((Vector.empty[ReadOperation], Vector.empty[DataOperation]) /: restOps) {
          case ((accRead, accOther), opRead: ReadOperation) => (accRead :+ opRead, accOther)
          case ((accRead, accOther), opOther: DataOperation) => (accRead, accOther :+ opOther)
        }

      val planId = UUID.randomUUID
      val plan = {
        val planExtra = Map[String, Any](
          ExecutionPlanExtra.DataTypes -> componentCreatorFactory.dataTypeConverter.values
        )

        val attributes = builders.map(_.outputAttributes).reduce(_ ++ _).distinct
        val expressions = Expressions(
          constants = builders.map(_.literals).reduce(_ ++ _).asOption,
          functions = builders.map(_.functionalExpressions).reduce(_ ++ _).asOption
        )

        val p = ExecutionPlan(
          id = planId.asOption,
          appName = ctx.session.sparkContext.appName.asOption,
          operations = Operations(writeOp, opReads.asOption, opOthers.asOption),
          attributes = attributes.asOption,
          expressions = expressions.asOption,
          systemInfo = NameAndVersion(AppMetaInfo.Spark, spark.SPARK_VERSION),
          agentInfo = NameAndVersion(AppMetaInfo.Spline, SplineBuildInfo.Version).asOption,
          extraInfo = planExtra.asOption
        )
        postProcessor.process(p)
      }

      if (writeCommand.mode == SaveMode.Ignore && iwdStrategy.wasWriteIgnored(writeMetrics)) {
        logDebug("Ignored write detected. Skipping lineage.")
        None
      }
      else {
        val errorOrDuration = result.toDisjunction.toEither
        val maybeError = errorOrDuration.left.toOption
        val maybeDuration = errorOrDuration.right.toOption

        val eventExtra = Map[String, Any](
          ExecutionEventExtra.AppId -> ctx.session.sparkContext.applicationId,
          ExecutionEventExtra.ReadMetrics -> readMetrics,
          ExecutionEventExtra.WriteMetrics -> writeMetrics
        )

        val ev = ExecutionEvent(
          planId = planId,
          timestamp = System.currentTimeMillis,
          durationNs = maybeDuration.map(_.toNanos),
          error = maybeError,
          extra = eventExtra.asOption)

        val event = postProcessor.process(ev)

        logDebug(s"Successfully harvested lineage from ${ctx.logicalPlan.getClass}")
        Some(plan -> event)
      }
    })
  }

  private def createOperationBuildersRecursively(rootOp: LogicalPlan): Seq[OperationNodeBuilder] = {
    @scala.annotation.tailrec
    def traverseAndCollect(
      accBuilders: Seq[OperationNodeBuilder],
      processedEntries: Map[LogicalPlan, OperationNodeBuilder],
      enqueuedEntries: Seq[(LogicalPlan, OperationNodeBuilder)]
    ): Seq[OperationNodeBuilder] = {
      enqueuedEntries match {
        case Nil => accBuilders
        case (curOpNode, parentBuilder) +: restEnqueuedEntries =>
          val maybeExistingBuilder = processedEntries.get(curOpNode)
          val curBuilder = maybeExistingBuilder.getOrElse(createOperationBuilder(curOpNode))

          if (parentBuilder != null) parentBuilder += curBuilder

          if (maybeExistingBuilder.isEmpty) {

            val newNodesToProcess = extractChildren(curOpNode)

            traverseAndCollect(
              curBuilder +: accBuilders,
              processedEntries + (curOpNode -> curBuilder),
              newNodesToProcess.map(_ -> curBuilder) ++ restEnqueuedEntries)

          } else {
            traverseAndCollect(accBuilders, processedEntries, restEnqueuedEntries)
          }
      }
    }

    val builders = traverseAndCollect(Nil, Map.empty, Seq((rootOp, null)))
    builders.sortedTopologicallyBy(_.id, _.childIds, reverse = true)
  }

  private def createOperationBuilder(op: LogicalPlan): OperationNodeBuilder =
    readCommandExtractor.asReadCommand(op)
      .map(opNodeBuilderFactory.readNodeBuilder)
      .getOrElse(opNodeBuilderFactory.genericNodeBuilder(op))

  private def extractChildren(plan: LogicalPlan) = plan match {
    case AnalysisBarrierExtractor(_) =>
      // special handling - spark 2.3 sometimes includes AnalysisBarrier in the plan
      val child = ReflectionUtils.extractFieldValue[LogicalPlan](plan, "child")
      Seq(child)

    case _ => plan.children
  }
}

object LineageHarvester {
  type Metrics = Map[String, Long]
  private type HarvestResult = Option[(ExecutionPlan, ExecutionEvent)]

  private def getExecutedReadWriteMetrics(executedPlan: SparkPlan): (Metrics, Metrics) = {
    def getNodeMetrics(node: SparkPlan): Metrics = node.metrics.mapValues(_.value)

    val cumulatedReadMetrics: Metrics = {
      @scala.annotation.tailrec
      def traverseAndCollect(acc: Metrics, nodes: Seq[SparkPlan]): Metrics = {
        nodes match {
          case Nil => acc
          case (leaf: LeafExecNode) +: queue =>
            traverseAndCollect(acc |+| getNodeMetrics(leaf), queue)
          case (node: SparkPlan) +: queue =>
            traverseAndCollect(acc, node.children ++ queue)
        }
      }

      traverseAndCollect(Map.empty, Seq(executedPlan))
    }

    (cumulatedReadMetrics, getNodeMetrics(executedPlan))
  }

  object AnalysisBarrierExtractor extends SafeTypeMatchingExtractor[LogicalPlan](
    "org.apache.spark.sql.catalyst.plans.logical.AnalysisBarrier")

}
