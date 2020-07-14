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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.commons.config.ConfigurationImplicits
import za.co.absa.spline.harvester
import za.co.absa.spline.harvester.builder.read.ReadRelationHandler
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode
import za.co.absa.spline.harvester.dispatcher.{AggregateLineageDispatcher, LineageDispatcher}
import za.co.absa.spline.harvester.extra.UserExtraMetadataProvider
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.{LineageHarvesterFactory, QueryExecutionEventHandler}

import scala.reflect.ClassTag

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
     * Lineage dispatcher used to report lineages
     */
    val LineageDispatcherClass = "spline.lineage_dispatcher.className"

    /**
     * Secondary lineage dispatchers used to perform arbitrary actions.  This is a comma-separated list.
     */
    val SecondaryLineageDispatcherClasses = "spline.lineage_dispatcher.secondary.classNames"

    /**
     * Relation handler used to deal with proprietary relations
     */
    val RelationHandlerClass = "spline.read_relation_handler.className"

    /**
     * Strategy used to detect ignored writes
     */
    val IgnoreWriteDetectionStrategyClass = "spline.iwd_strategy.className"

    /**
     * Which strategy should be used to detect mode=ignore writes
     */
    val UserExtraMetadataProviderClass = "spline.user_extra_meta_provider.className"
  }

  def apply(sparkSession: SparkSession): DefaultSplineConfigurer = {
    new DefaultSplineConfigurer(StandardSplineConfigurationStack(sparkSession))
  }
}

class DefaultSplineConfigurer(userConfiguration: Configuration)
  extends SplineConfigurer
    with Logging {

  import ConfigurationImplicits._
  import DefaultSplineConfigurer.ConfProperty._
  import DefaultSplineConfigurer._
  import SplineMode._

  import collection.JavaConverters._

  private lazy val configuration = new CompositeConfiguration(Seq(
    userConfiguration,
    new PropertiesConfiguration(defaultPropertiesFileName)
  ).asJava)

  lazy val splineMode: SplineMode = {
    val modeName: String = configuration.getRequiredString(Mode)
    try SplineMode withName modeName
    catch {
      case _: NoSuchElementException => throw new IllegalArgumentException(
        s"Invalid value for property $Mode=$modeName. Should be one of: ${SplineMode.values mkString ", "}")
    }
  }

  private def instantiate[T: ClassTag](className: String): T = harvester.instantiate(configuration, className)

  override def queryExecutionEventHandler: QueryExecutionEventHandler =
    new QueryExecutionEventHandler(harvesterFactory, lineageDispatcher)

  protected def relationHandler: ReadRelationHandler = instantiate[ReadRelationHandler](
    configuration.getString(RelationHandlerClass,
                            "za.co.absa.spline.harvester.builder.read.NoOpReadRelationHandler"))

  protected[conf] def lineageDispatcher: LineageDispatcher = {
    val dispatcherClassName: String = configuration.getRequiredString(LineageDispatcherClass)
    val primaryDispatcher: LineageDispatcher = instantiate[LineageDispatcher](dispatcherClassName)

    val secondaryDispatcherClassNames: Array[String] = configuration.getStringArray(
      SecondaryLineageDispatcherClasses)
    if (secondaryDispatcherClassNames.nonEmpty) {
      val secondaryDispatchers: Array[LineageDispatcher] =
        for {className <- secondaryDispatcherClassNames} yield instantiate[LineageDispatcher](className)
      new AggregateLineageDispatcher(primaryDispatcher,
                                     Option(secondaryDispatchers))
    } else {
      primaryDispatcher
    }
  }

  protected def ignoredWriteDetectionStrategy: IgnoredWriteDetectionStrategy = instantiate[IgnoredWriteDetectionStrategy](
    configuration.getRequiredString(IgnoreWriteDetectionStrategyClass))

  protected def userExtraMetadataProvider: UserExtraMetadataProvider = instantiate[UserExtraMetadataProvider](
    configuration.getRequiredString(UserExtraMetadataProviderClass))

  private def harvesterFactory = new LineageHarvesterFactory(
    splineMode,
    ignoredWriteDetectionStrategy,
    userExtraMetadataProvider,
    relationHandler)
}
