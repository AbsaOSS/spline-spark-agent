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
 * The object contains logic needed for initialization of the library
 */
object SparkLineageInitializer extends Logging {

  def enableLineageTracking(sparkSession: SparkSession): SparkSession =
    SparkSessionWrapper(sparkSession).enableLineageTracking()

  def enableLineageTracking(sparkSession: SparkSession, configurer: SplineConfigurer): SparkSession =
    SparkSessionWrapper(sparkSession).enableLineageTracking(configurer)

  implicit class SparkSessionWrapper(sparkSession: SparkSession) {

    private implicit val executionContext: ExecutionContext = ExecutionContext.global

    /**
     * The method performs all necessary registrations and procedures for initialization of the library.
     *
     * @param configurer A collection of settings for the library initialization
     * @return An original Spark session
     */
    def enableLineageTracking(configurer: SplineConfigurer = DefaultSplineConfigurer(sparkSession)): SparkSession = {
      new QueryExecutionEventHandlerFactory(sparkSession)
        .createEventHandler(configurer, false)
        .foreach(eventHandler =>
          sparkSession.listenerManager.register(new SplineQueryExecutionListener(Some(eventHandler))))

      sparkSession
    }
  }

  val InitFlagKey = "spline.initializedFlag"

  val SparkQueryExecutionListenersKey = "spark.sql.queryExecutionListeners"
}
