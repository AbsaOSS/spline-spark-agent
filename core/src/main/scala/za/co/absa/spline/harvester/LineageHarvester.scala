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

import java.util.UUID

import org.apache.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import scalaz.Scalaz._
import za.co.absa.commons.lang.OptionImplicits._
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.ExtraMetadataImplicits._
import za.co.absa.spline.harvester.LineageHarvester._
import za.co.absa.spline.harvester.ModelConstants.{AppMetaInfo, ExecutionEventExtra, ExecutionPlanExtra}
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.builder.read.ReadCommandExtractor
import za.co.absa.spline.harvester.builder.write.WriteCommandExtractor
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode.SplineMode
import za.co.absa.spline.harvester.converter.ExpressionConverter
import za.co.absa.spline.harvester.extra.UserExtraMetadataProvider
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.logging.ObjectStructureDumper
import za.co.absa.spline.producer.model.v1_1._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class LineageHarvester(
  ctx: HarvestingContext,
  splineMode: SplineMode,
  writeCommandExtractor: WriteCommandExtractor,
  readCommandExtractor: ReadCommandExtractor,
  iwdStrategy: IgnoredWriteDetectionStrategy,
  userExtraMetadataProvider: UserExtraMetadataProvider
) extends Logging {

  private val componentCreatorFactory: ComponentCreatorFactory = new ComponentCreatorFactory
  private val opNodeBuilderFactory = new OperationNodeBuilderFactory(userExtraMetadataProvider, componentCreatorFactory, ctx)

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

      val writeOp = writeOpBuilder.build()
      val restOps = restOpBuilders.map(_.build())

      val (opReads, opOthers) =
        ((List.empty[ReadOperation], List.empty[DataOperation]) /: restOps) {
          case ((accRead, accOther), opRead: ReadOperation) => (accRead :+ opRead, accOther)
          case ((accRead, accOther), opOther: DataOperation) => (accRead, accOther :+ opOther)
        }

      val plan = {
        val planExtra = Map[String, Any](
          ExecutionPlanExtra.AppName -> ctx.session.sparkContext.appName,
          ExecutionPlanExtra.DataTypes -> componentCreatorFactory.dataTypeConverter.values
        )

        val expressions = createExpressions(
          componentCreatorFactory.expressionConverter.values,
          componentCreatorFactory.inputAttributeConverter.values
        )

        val p = ExecutionPlan(
          id = Some(UUID.randomUUID),
          operations = Operations(writeOp, opReads.asOption, opOthers.asOption),
          expressions = Some(expressions),
          systemInfo = NameAndVersion(AppMetaInfo.Spark, spark.SPARK_VERSION),
          agentInfo = Some(NameAndVersion(AppMetaInfo.Spline, SplineBuildInfo.Version)),
          extraInfo = planExtra.asOption
        )
        p.withAddedExtra(userExtraMetadataProvider.forExecPlan(p, ctx))
      }

      if (writeCommand.mode == SaveMode.Ignore && iwdStrategy.wasWriteIgnored(writeMetrics)) {
        logDebug("Ignored write detected. Skipping lineage.")
        None
      }
      else {
        val eventExtra = Map[String, Any](
          ExecutionEventExtra.AppId -> ctx.session.sparkContext.applicationId,
          ExecutionEventExtra.ReadMetrics -> readMetrics,
          ExecutionEventExtra.WriteMetrics -> writeMetrics
        ).optionally[Duration]((m, d) => m + (ExecutionEventExtra.DurationNs -> d.toNanos), result.toOption)

        val ev = ExecutionEvent(
          planId = plan.id.get,
          timestamp = System.currentTimeMillis,
          error = result.left.toOption,
          extra = eventExtra.asOption)

        val eventUserExtra = userExtraMetadataProvider.forExecEvent(ev, ctx)
        val event = ev.withAddedExtra(eventUserExtra)

        logDebug(s"Successfully harvested lineage from ${ctx.logicalPlan.getClass}")
        Some(plan -> event)
      }
    })
  }

  private def createExpressions(
    unsortedExpressions: Seq[ExpressionConverter.ExpressionLike],
    inputAttributes: Seq[Attribute]
  ): Expressions = {

    val exprByType = unsortedExpressions.toList.groupBy(i => i.asInstanceOf[Any].getClass().getSimpleName)

    def getExprByType[T](simpleClassName: String): List[T] = {
      exprByType
        .get(simpleClassName)
        .map(_.asInstanceOf[List[T]])
        .getOrElse(List.empty[T])
    }

    Expressions(
      attributes = inputAttributes.toList ::: getExprByType[Attribute]("Attribute"),
      constants = getExprByType[Literal]("Literal"),
      functions = getExprByType[FunctionalExpression]("FunctionalExpression")
    )
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

    traverseAndCollect(Nil, Map.empty, Seq((rootOp, null)))
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
