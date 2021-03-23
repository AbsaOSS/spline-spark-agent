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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.spline.harvester.conf.{DefaultSplineConfigurer, SplineConfigurer}
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener

import scala.concurrent.ExecutionContext

/**
 * Spline agent initializer
 */
object SparkLineageInitializer extends Logging {

  def enableLineageTracking(sparkSession: SparkSession): SparkSession =
    enableLineageTracking(sparkSession, DefaultSplineConfigurer(sparkSession))

  /**
   * Enable lineage tracking for the given Spark Session.
   * This is an alternative method to so called "codeless initialization" (via using `spark.sql.queryExecutionListeners`
   * config parameter). Use either of those, not both.
   *
   * The programmatic approach provides a higher level of customization in comparison to the declarative (codeless) one,
   * but on the other hand for the majority of practical purposes the codeless initialization is sufficient and
   * provides a lower coupling between the Spline agent and the target application by not requiring any dependency
   * on Spline agent library from the client source code. Thus we recommend to prefer a codeless init, rather than
   * calling this method directly unless you have specific customization requirements.
   *
   * (See the Spline agent doc for details)
   *
   * @param sparkSession a Spark Session on which the lineage tracking should be enabled.
   * @param configurer   A collection of settings for the library initialization
   * @return An original Spark session
   */
  def enableLineageTracking(sparkSession: SparkSession, configurer: SplineConfigurer): SparkSession = {
    new QueryExecutionEventHandlerFactory(sparkSession)
      .createEventHandler(configurer, isCodelessInit = false)
      .foreach(eventHandler =>
        sparkSession.listenerManager.register(new SplineQueryExecutionListener(Some(eventHandler))))

    sparkSession
  }

  /**
   * Allows for the fluent DSL like this:
   * {{{
   *   sparkSession
   *     .enableLineageTracking(...)
   *     .read
   *     .parquet(...)
   *     .etc
   * }}}
   *
   * @param sparkSession a Spark Session on which the lineage tracking should be enabled.
   */
  implicit class SplineSparkSessionWrapper(sparkSession: SparkSession) {

    private implicit val executionContext: ExecutionContext = ExecutionContext.global

    def enableLineageTracking(configurer: SplineConfigurer = DefaultSplineConfigurer(sparkSession)): SparkSession = {
      SparkLineageInitializer.enableLineageTracking(sparkSession, configurer)
    }
  }

  val InitFlagKey = "spline.initializedFlag"

  val SparkQueryExecutionListenersKey = "spark.sql.queryExecutionListeners"
}
