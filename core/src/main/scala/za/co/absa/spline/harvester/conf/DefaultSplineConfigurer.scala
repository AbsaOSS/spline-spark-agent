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

package za.co.absa.spline.harvester.conf

import org.apache.commons.configuration.{CompositeConfiguration, Configuration, PropertiesConfiguration}
import org.apache.spark.sql.SparkSession
import za.co.absa.commons.config.ConfigurationImplicits
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode.SplineMode
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.extra.{UserExtraAppendingLineageFilter, UserExtraMetadataProvider}
import za.co.absa.spline.harvester.iwd.DefaultIgnoredWriteDetectionStrategy.Behaviour
import za.co.absa.spline.harvester.iwd.{DefaultIgnoredWriteDetectionStrategy, IgnoredWriteDetectionStrategy}
import za.co.absa.spline.harvester.postprocessing.LineageFilter
import za.co.absa.spline.harvester.{LineageHarvesterFactory, QueryExecutionEventHandler}

object DefaultSplineConfigurer {
  private val defaultPropertiesFileName = "spline.default.properties"

  object ConfProperty {

    /**
     * How Spline should behave.
     *
     * @see [[SplineMode]]
     */
    val Mode = "spline.mode"

    /**
     * Lineage dispatcher name - defining namespace for rest of properties for that dispatcher
     */
    val RootLineageDispatcher = "spline.lineageDispatcher"

    /**
     * Strategy used to detect ignored writes
     */
    val IgnoreWriteDetectionStrategyClass = "spline.IWDStrategy.className"

    /**
     * User defined filters: allowing modification and enrichment of generated lineage
     */
    val PostProcessingFilterClasses = "spline.postprocessingFilter.classNames"

    /**
     * Deprecated - use Post Processing Filter instead
     */
    val UserExtraMetadataProviderClass = "spline.userExtraMetaProvider.className"
  }

  def apply(sparkSession: SparkSession): DefaultSplineConfigurer = {
    new DefaultSplineConfigurer(sparkSession, StandardSplineConfigurationStack(sparkSession))
  }
}

class DefaultSplineConfigurer(sparkSession: SparkSession, userConfiguration: Configuration) extends SplineConfigurer {

  import ConfigurationImplicits._
  import DefaultSplineConfigurer.ConfProperty._
  import DefaultSplineConfigurer._
  import SplineMode._

  import collection.JavaConverters._

  private lazy val configuration = new CompositeConfiguration(Seq(
    userConfiguration,
    new Spline05ConfigurationAdapter(userConfiguration),
    new PropertiesConfiguration(defaultPropertiesFileName)
  ).asJava)

  private lazy val objectFactory = new HierarchicalObjectFactory(configuration)

  lazy val splineMode: SplineMode = {
    val modeName = configuration.getRequiredString(Mode)
    try SplineMode withName modeName
    catch {
      case _: NoSuchElementException => throw new IllegalArgumentException(
        s"Invalid value for property $Mode=$modeName. Should be one of: ${SplineMode.values mkString ", "}")
    }
  }

  override def queryExecutionEventHandler: QueryExecutionEventHandler =
    new QueryExecutionEventHandler(harvesterFactory, lineageDispatcher)

  protected def lineageDispatcher: LineageDispatcher = {
    val dispatcherName = configuration.getRequiredString(RootLineageDispatcher)
    objectFactory
      .child(RootLineageDispatcher)
      .child(dispatcherName)
      .instantiate[LineageDispatcher]()
  }

  protected def ignoredWriteDetectionStrategy: IgnoredWriteDetectionStrategy =
    objectFactory.instantiate[IgnoredWriteDetectionStrategy](configuration.getRequiredString(IgnoreWriteDetectionStrategyClass))

  protected def postProcessingFilters: Seq[LineageFilter] =
    configuration
      .getStringArray(PostProcessingFilterClasses)
      .filter(_.nonEmpty)
      .map(objectFactory.instantiate[LineageFilter])

  protected def maybeUserExtraMetadataProvider: Option[UserExtraMetadataProvider] =
    configuration
      .getOptionalString(UserExtraMetadataProviderClass)
      .map(objectFactory.instantiate[UserExtraMetadataProvider])

  private def harvesterFactory = new LineageHarvesterFactory(
    sparkSession,
    splineMode,
    ignoredWriteDetectionStrategy,
    maybeUserExtraMetadataProvider
      .map(postProcessingFilters :+ new UserExtraAppendingLineageFilter(_))
      .getOrElse(postProcessingFilters)
  )
}

case class CodeBasedSplineConfigurer(
  sparkSession: SparkSession,
  lineageDispatcher: LineageDispatcher,
  splineMode: SplineMode = SplineMode.BEST_EFFORT,
  ignoredWriteDetectionStrategy: IgnoredWriteDetectionStrategy = new DefaultIgnoredWriteDetectionStrategy(true),
  postProcessingFilters: Seq[LineageFilter] = Seq.empty,
) extends SplineConfigurer {

  override def queryExecutionEventHandler: QueryExecutionEventHandler =
    new QueryExecutionEventHandler(harvesterFactory, lineageDispatcher)

  private def harvesterFactory = new LineageHarvesterFactory(
    sparkSession,
    splineMode,
    ignoredWriteDetectionStrategy,
    postProcessingFilters
  )
}
