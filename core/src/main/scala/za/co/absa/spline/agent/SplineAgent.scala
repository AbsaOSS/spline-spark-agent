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

package za.co.absa.spline.agent

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.commons.lang.CachingConverter
import za.co.absa.spline.harvester.IdGenerator.{UUIDGeneratorFactory, UUIDNamespace}
import za.co.absa.spline.harvester.builder.OperationNodeBuilderFactory
import za.co.absa.spline.harvester.builder.dsformat.PluggableDataSourceFormatResolver
import za.co.absa.spline.harvester.builder.read.PluggableReadCommandExtractor
import za.co.absa.spline.harvester.builder.write.PluggableWriteCommandExtractor
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.plugin.registry.AutoDiscoveryPluginRegistry
import za.co.absa.spline.harvester.postprocessing._
import za.co.absa.spline.harvester.qualifier.HDFSPathQualifier
import za.co.absa.spline.harvester.{HarvestingContext, IdGeneratorsBundle, LineageHarvester}
import za.co.absa.spline.producer.model.ExecutionPlan

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

private[spline] trait SplineAgent {
  def handle(funcName: FuncName, qe: QueryExecution, result: Either[Throwable, Duration]): Unit
}

object SplineAgent extends Logging {

  type FuncName = String

  private val InternalPostProcessingFilters = Seq(
    new AttributeReorderingFilter,
    new OneRowRelationFilter,
    new ViewAttributeAddingFilter
  )

  def create(
    pluginsConfig: Configuration,
    session: SparkSession,
    lineageDispatcher: LineageDispatcher,
    userPostProcessingFilter: Option[PostProcessingFilter],
    iwdStrategy: IgnoredWriteDetectionStrategy,
    execPlanUUIDGeneratorFactory: UUIDGeneratorFactory[UUIDNamespace, ExecutionPlan]): SplineAgent = {

    val filters = InternalPostProcessingFilters ++ userPostProcessingFilter
    val pathQualifier = new HDFSPathQualifier(session.sparkContext.hadoopConfiguration)
    val pluginRegistry = new AutoDiscoveryPluginRegistry(pluginsConfig, pathQualifier, session)
    val dataSourceFormatResolver = new PluggableDataSourceFormatResolver(pluginRegistry)
    val writeCommandExtractor = new PluggableWriteCommandExtractor(pluginRegistry, dataSourceFormatResolver)
    val readCommandExtractor = new PluggableReadCommandExtractor(pluginRegistry, dataSourceFormatResolver)

    new SplineAgent {
      def handle(funcName: FuncName, qe: QueryExecution, result: Either[Throwable, Duration]): Unit = withErrorHandling {
        val idGenerators = new IdGeneratorsBundle(execPlanUUIDGeneratorFactory)
        val harvestingContext = new HarvestingContext(funcName, qe.analyzed, Some(qe.executedPlan), session, idGenerators)
        val postProcessor = new PostProcessor(filters, harvestingContext)
        val dataTypeConverter = new DataTypeConverter(idGenerators.dataTypeIdGenerator) with CachingConverter
        val dataConverter = new DataConverter
        val opNodeBuilderFactory = new OperationNodeBuilderFactory(postProcessor, dataTypeConverter, dataConverter, idGenerators)

        val harvester = new LineageHarvester(
          harvestingContext,
          writeCommandExtractor,
          readCommandExtractor,
          iwdStrategy,
          postProcessor,
          dataTypeConverter,
          opNodeBuilderFactory
        )

        harvester
          .harvest(result)
          .foreach({
            case (plan, event) =>
              lineageDispatcher.send(plan)
              lineageDispatcher.send(event)
          })
      }

      private def withErrorHandling(body: => Unit): Unit = {
        try body
        catch {
          case NonFatal(e) =>
            val ctx = session.sparkContext
            logError(s"Unexpected error occurred during lineage processing for application: ${ctx.appName} #${ctx.applicationId}", e)
        }
      }
    }
  }
}
