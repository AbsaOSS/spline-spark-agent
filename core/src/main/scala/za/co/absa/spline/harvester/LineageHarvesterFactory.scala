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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import za.co.absa.commons.lang.CachingConverter
import za.co.absa.spline.harvester.IdGenerator.{UUIDGeneratorFactory, UUIDNamespace}
import za.co.absa.spline.harvester.builder.OperationNodeBuilderFactory
import za.co.absa.spline.harvester.builder.dsformat.PluggableDataSourceFormatResolver
import za.co.absa.spline.harvester.builder.read.PluggableReadCommandExtractor
import za.co.absa.spline.harvester.builder.write.PluggableWriteCommandExtractor
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode.SplineMode
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.plugin.registry.AutoDiscoveryPluginRegistry
import za.co.absa.spline.harvester.postprocessing.{PostProcessingFilter, PostProcessor}
import za.co.absa.spline.harvester.qualifier.HDFSPathQualifier
import za.co.absa.spline.producer.model.v1_1.ExecutionPlan

import scala.language.postfixOps

class LineageHarvesterFactory(
  session: SparkSession,
  splineMode: SplineMode,
  execPlanUUIDGeneratorFactory: UUIDGeneratorFactory[UUIDNamespace, ExecutionPlan],
  iwdStrategy: IgnoredWriteDetectionStrategy,
  filters: Seq[PostProcessingFilter]) {

  private val pathQualifier = new HDFSPathQualifier(session.sparkContext.hadoopConfiguration)
  private val pluginRegistry = new AutoDiscoveryPluginRegistry(pathQualifier, session)
  private val dataSourceFormatResolver = new PluggableDataSourceFormatResolver(pluginRegistry)
  private val writeCommandExtractor = new PluggableWriteCommandExtractor(pluginRegistry, dataSourceFormatResolver)
  private val readCommandExtractor = new PluggableReadCommandExtractor(pluginRegistry, dataSourceFormatResolver)

  def harvester(logicalPlan: LogicalPlan, executedPlan: Option[SparkPlan]): LineageHarvester = {
    val idGenerators = new IdGenerators(execPlanUUIDGeneratorFactory)
    val harvestingContext = new HarvestingContext(logicalPlan, executedPlan, session, idGenerators)
    val postProcessor = new PostProcessor(filters, harvestingContext)
    val dataTypeConverter = new DataTypeConverter(idGenerators.dataTypeIdGenerator) with CachingConverter
    val dataConverter = new DataConverter
    val opNodeBuilderFactory = new OperationNodeBuilderFactory(postProcessor, dataTypeConverter, dataConverter, idGenerators)

    new LineageHarvester(
      harvestingContext,
      splineMode,
      writeCommandExtractor,
      readCommandExtractor,
      iwdStrategy,
      postProcessor,
      dataTypeConverter,
      opNodeBuilderFactory
    )
  }

}
