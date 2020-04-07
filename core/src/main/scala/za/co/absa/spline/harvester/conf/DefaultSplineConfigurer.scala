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
import za.co.absa.spline.harvester.{LineageHarvesterFactory, QueryExecutionEventHandler}

object DefaultSplineConfigurer {
  private val defaultPropertiesFileName = "spline.default.properties"

  object ConfProperty {

    /**
     * How Spline should behave.
     *
     * @see [[SplineMode]]
     */
    val MODE = "spline.mode"

    /**
     * Which lineage dispatcher should be used to report lineages
     */
    val LINEAGE_DISPATCHER_CLASS = "spline.lineage_dispatcher.className"
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
    val modeName = configuration.getRequiredString(MODE)
    try SplineMode withName modeName
    catch {
      case _: NoSuchElementException => throw new IllegalArgumentException(
        s"Invalid value for property $MODE=$modeName. Should be one of: ${SplineMode.values mkString ", "}")
    }
  }

  override lazy val lineageDispatcher: LineageDispatcher = {
    val className = configuration.getRequiredString(LINEAGE_DISPATCHER_CLASS)
    log debug s"Instantiating a lineage dispatcher for class name: $className"
    try {
      Class.forName(className.trim)
        .getConstructor(classOf[Configuration])
        .newInstance(configuration)
        .asInstanceOf[LineageDispatcher]
    }
    catch {
      case e: InvocationTargetException => throw e.getTargetException
    }
  }

  private lazy val harvesterFactory = new LineageHarvesterFactory(splineMode)

  def queryExecutionEventHandler: QueryExecutionEventHandler =
    new QueryExecutionEventHandler(harvesterFactory, lineageDispatcher)
}
