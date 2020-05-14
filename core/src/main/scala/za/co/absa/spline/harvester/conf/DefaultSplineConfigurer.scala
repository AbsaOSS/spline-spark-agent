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

import java.lang.reflect.InvocationTargetException

import org.apache.commons.configuration.{CompositeConfiguration, Configuration, PropertiesConfiguration}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.commons.config.ConfigurationImplicits
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.write_detection.IgnoredWriteDetectionStrategy
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
     * Which lineage dispatcher should be used to report lineages
     */
    val LineageDispatcherClass = "spline.lineage_dispatcher.className"

    /**
     * Which strategy should be used to detect mode=ignore writes
     */
    val IgnoreWriteDetectionStrategyClass = "spline.iwd_strategy.className"
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
    val modeName = configuration.getRequiredString(Mode)
    try SplineMode withName modeName
    catch {
      case _: NoSuchElementException => throw new IllegalArgumentException(
        s"Invalid value for property $Mode=$modeName. Should be one of: ${SplineMode.values mkString ", "}")
    }
  }

  override lazy val lineageDispatcher: LineageDispatcher = instantiate[LineageDispatcher](
    configuration.getRequiredString(LineageDispatcherClass))

  private lazy val ignoreWriteDetectionStrategy = instantiate[IgnoredWriteDetectionStrategy](
    configuration.getRequiredString(IgnoreWriteDetectionStrategyClass))

  private def instantiate[T: ClassTag](className: String): T = {
    val interfaceName = scala.reflect.classTag[T].runtimeClass.getSimpleName
    log debug s"Instantiating ${interfaceName} for class name: $className"
    try {
      Class.forName(className.trim)
        .getConstructor(classOf[Configuration])
        .newInstance(configuration)
        .asInstanceOf[T]
    }
    catch {
      case e: InvocationTargetException => throw e.getTargetException
    }
  }

  private lazy val harvesterFactory = new LineageHarvesterFactory(splineMode, ignoreWriteDetectionStrategy)

  def queryExecutionEventHandler: QueryExecutionEventHandler =
    new QueryExecutionEventHandler(harvesterFactory, lineageDispatcher)
}
