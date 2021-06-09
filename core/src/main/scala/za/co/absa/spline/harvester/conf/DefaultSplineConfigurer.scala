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
import za.co.absa.commons.HierarchicalObjectFactory
import za.co.absa.commons.config.ConfigurationImplicits
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.extra.{UserExtraAppendingPostProcessingFilter, UserExtraMetadataProvider}
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.postprocessing.PostProcessingFilter
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
     * Lineage dispatcher name - defining namespace for rest of properties for that dispatcher
     */
    val RootLineageDispatcher = "spline.lineageDispatcher"

    /**
     * User defined filter: allowing modification and enrichment of generated lineage
     */
    val RootPostProcessingFilter = "spline.postProcessingFilter"

    /**
     * Strategy used to detect ignored writes
     */
    val IgnoreWriteDetectionStrategy = "spline.IWDStrategy"

    /**
     * @deprecated use Post Processing Filter instead
     */
    val UserExtraMetadataProviderClass = "spline.userExtraMetaProvider.className"
  }

  def apply(sparkSession: SparkSession): DefaultSplineConfigurer = {
    new DefaultSplineConfigurer(sparkSession, StandardSplineConfigurationStack(sparkSession))
  }
}

class DefaultSplineConfigurer(sparkSession: SparkSession, userConfiguration: Configuration)
  extends SplineConfigurer
    with Logging {

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

  override def queryExecutionEventHandler: QueryExecutionEventHandler = {
    logInfo(s"Lineage Dispatcher: ${configuration.getString(RootLineageDispatcher)}")
    logInfo(s"Post-Processing Filter: ${configuration.getString(RootPostProcessingFilter)}")
    logInfo(s"Ignore-Write Detection Strategy: ${configuration.getString(IgnoreWriteDetectionStrategy)}")

    new QueryExecutionEventHandler(harvesterFactory, lineageDispatcher)
  }

  protected def lineageDispatcher: LineageDispatcher = createComponentByKey(RootLineageDispatcher)

  protected def postProcessingFilter: PostProcessingFilter = createComponentByKey(RootPostProcessingFilter)

  protected def ignoredWriteDetectionStrategy: IgnoredWriteDetectionStrategy = createComponentByKey(IgnoreWriteDetectionStrategy)

  protected def maybeUserExtraMetadataProvider: Option[UserExtraMetadataProvider] =
    configuration
      .getOptionalString(UserExtraMetadataProviderClass)
      .map(objectFactory.instantiate[UserExtraMetadataProvider])

  private def createComponentByKey[A: ClassTag](key: String): A = {
    val objName = configuration.getRequiredString(key)
    objectFactory
      .child(key)
      .child(objName)
      .instantiate[A]()
  }

  private def harvesterFactory = new LineageHarvesterFactory(
    sparkSession,
    splineMode,
    ignoredWriteDetectionStrategy,
    maybeUserExtraMetadataProvider
      .map(uemp => Seq(postProcessingFilter, new UserExtraAppendingPostProcessingFilter(uemp)))
      .getOrElse(Seq(postProcessingFilter))
  )
}
