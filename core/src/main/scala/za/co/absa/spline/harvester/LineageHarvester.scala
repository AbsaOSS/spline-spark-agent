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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.commons.CollectionImplicits._
import za.co.absa.spline.commons.graph.GraphImplicits._
import za.co.absa.spline.commons.lang.CachingConverter
import za.co.absa.spline.harvester.LineageHarvester._
import za.co.absa.spline.harvester.ModelConstants.{AppMetaInfo, ExecutionEventExtra, ExecutionPlanExtra}
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.builder.read.ReadCommandExtractor
import za.co.absa.spline.harvester.builder.write.{WriteCommand, WriteCommandExtractor}
import za.co.absa.spline.harvester.converter.DataTypeConverter
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.logging.ObjectStructureLogging
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class LineageHarvester(
  ctx: HarvestingContext,
  writeCommandExtractor: WriteCommandExtractor,
  readCommandExtractor: ReadCommandExtractor,
  iwdStrategy: IgnoredWriteDetectionStrategy,
  postProcessor: PostProcessor,
  dataTypeConverter: DataTypeConverter with CachingConverter,
  opNodeBuilderFactory: OperationNodeBuilderFactory
) extends Logging with ObjectStructureLogging {

  def harvest(result: Either[Throwable, Duration]): HarvestResult = {
    logDebug(s"Harvesting lineage from ${ctx.logicalPlan.getClass}")

    val (readMetrics: Metrics, writeMetrics: Metrics) = ctx.executedPlanOpt.
      map(getExecutedReadWriteMetrics).
      getOrElse((Map.empty, Map.empty))

    tryExtractWriteCommand(ctx.funcName, ctx.logicalPlan).flatMap(writeCommand => {
      val writeOpBuilder = opNodeBuilderFactory.writeNodeBuilder(writeCommand)
      val restOpBuilders = createOperationBuildersRecursively(writeCommand.query)

      restOpBuilders.lastOption.foreach(writeOpBuilder.addChild)
      val builders = restOpBuilders :+ writeOpBuilder

      val restOps = restOpBuilders.map(_.build())
      val writeOp = writeOpBuilder.build()

      val (opReads, opOthers) =
        restOps.foldLeft((Vector.empty[ReadOperation], Vector.empty[DataOperation])) {
          case ((accRead, accOther), opRead: ReadOperation) => (accRead :+ opRead, accOther)
          case ((accRead, accOther), opOther: DataOperation) => (accRead, accOther :+ opOther)
        }

      if (writeCommand.mode == SaveMode.Ignore && iwdStrategy.wasWriteIgnored(writeMetrics)) {
        logDebug("Ignored write detected. Skipping lineage.")
        None
      }
      else {
        val planWithoutId = {
          val planExtra = Map[String, Any](
            ExecutionPlanExtra.AppName -> ctx.session.conf.get("spark.app.name"),
            ExecutionPlanExtra.DataTypes -> dataTypeConverter.values
          )

          val attributes = (builders.map(_.outputAttributes) :+ writeOpBuilder.additionalAttributes)
            .reduce(_ ++ _)
            .distinct

          val expressions = Expressions(
            constants = builders.map(_.literals).reduce(_ ++ _),
            functions = builders.map(_.functionalExpressions).reduce(_ ++ _)
          )

          val p = ExecutionPlan(
            id = None,
            discriminator = None,
            labels = Map.empty,
            name = ctx.session.conf.get("spark.app.name"), // `appName` for now, but could be different (user defined) in the future
            operations = Operations(writeOp, opReads, opOthers),
            attributes = attributes,
            expressions = expressions,
            systemInfo = SparkVersionInfo,
            agentInfo = SplineVersionInfo,
            extraInfo = planExtra
          )
          postProcessor.process(p)
        }

        val planId = ctx.idGenerators.execPlanIdGenerator.nextId(planWithoutId)
        val plan = planWithoutId.copy(id = Some(planId))

        val event = {
          val maybeDurationNs = result.right.toOption.map(_.toNanos)
          val maybeErrorString = result.left.toOption.map(ExceptionUtils.getStackTrace)

          val eventExtra = Map[String, Any](
            ExecutionEventExtra.AppId -> ctx.session.sparkContext.applicationId,
            ExecutionEventExtra.User -> ctx.session.sparkContext.sparkUser,
            ExecutionEventExtra.ReadMetrics -> readMetrics,
            ExecutionEventExtra.WriteMetrics -> writeMetrics
          )

          val ev = ExecutionEvent(
            planId = planId,
            discriminator = None,
            labels = Map.empty,
            timestamp = System.currentTimeMillis,
            durationNs = maybeDurationNs,
            error = maybeErrorString,
            extra = eventExtra)

          postProcessor.process(ev)
        }

        logDebug(s"Successfully harvested lineage from ${ctx.logicalPlan.getClass}")
        Some(plan -> event)
      }
    })
  }

  private def tryExtractWriteCommand(funcName: FuncName, plan: LogicalPlan): Option[WriteCommand] =
    Try(writeCommandExtractor.asWriteCommand(funcName, plan)) match {
      case Success(Some(write)) => Some(write)
      case Success(None) =>
        logDebug(s"${plan.getClass} was not recognized as a write-command. Skipping.")
        logObjectStructureAsTrace(plan)
        None
      case Failure(e) =>
        logObjectStructureAsError(plan)
        throw new RuntimeException(s"Write extraction failed for: ${plan.getClass}", e)
    }

  private def createOperationBuildersRecursively(rootOp: LogicalPlan): Seq[OperationNodeBuilder] = {
    @scala.annotation.tailrec
    def traverseAndCollect(
      accBuilders: Seq[OperationNodeBuilder],
      processedEntries: Map[PlanOrRdd, OperationNodeBuilder],
      enqueuedEntries: Seq[(PlanOrRdd, OperationNodeBuilder)]
    ): Seq[OperationNodeBuilder] = {
      enqueuedEntries match {
        case Nil => accBuilders
        case (curOpNode, parentBuilder) +: restEnqueuedEntries =>
          val maybeExistingBuilder = processedEntries.get(curOpNode)
          val curBuilder = maybeExistingBuilder.getOrElse(createOperationBuilder(curOpNode))

          if (parentBuilder != null) parentBuilder.addChild(curBuilder)

          if (maybeExistingBuilder.isEmpty) {

            val newNodesToProcess = opNodeBuilderFactory.nodeChildren(curOpNode)

            traverseAndCollect(
              curBuilder +: accBuilders,
              processedEntries + (curOpNode -> curBuilder),
              newNodesToProcess.map(_ -> curBuilder) ++ restEnqueuedEntries)

          } else {
            traverseAndCollect(accBuilders, processedEntries, restEnqueuedEntries)
          }
      }
    }

    val builders = traverseAndCollect(Nil, Map.empty, Seq((PlanWrap(rootOp), null)))
    builders.sortedTopologicallyBy(_.operationId, _.childIds, reverse = true)
  }

  private def createOperationBuilder(por: PlanOrRdd): OperationNodeBuilder =
    readCommandExtractor.asReadCommand(por)
      .map(rc => opNodeBuilderFactory.readNodeBuilder(rc, por))
      .getOrElse(opNodeBuilderFactory.genericNodeBuilder(por))

}

object LineageHarvester {

  import za.co.absa.spline.commons.version.Version

  trait PlanOrRdd

  case class PlanWrap(plan: LogicalPlan) extends PlanOrRdd

  case class RddWrap(rdd: RDD[_]) extends PlanOrRdd

  val SparkVersionInfo: NameAndVersion = NameAndVersion(
    name = AppMetaInfo.Spark,
    version = spark.SPARK_VERSION
  )

  val SplineVersionInfo: NameAndVersion = NameAndVersion(
    name = AppMetaInfo.Spline,
    version = {
      val splineSemver = Version.asSemVer(SplineBuildInfo.Version)
      if (splineSemver.preRelease.isEmpty) SplineBuildInfo.Version
      else s"${SplineBuildInfo.Version}+${SplineBuildInfo.Revision}"
    }
  )

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

}
