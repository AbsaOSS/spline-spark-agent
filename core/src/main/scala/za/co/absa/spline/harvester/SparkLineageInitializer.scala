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

import org.apache.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode._
import za.co.absa.spline.harvester.conf.{DefaultSplineConfigurer, SplineConfigurer}
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

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
      val splineConfiguredForCodelessInit = sparkSession.sparkContext.getConf
        .getOption(SparkQueryExecutionListenersKey).toSeq
        .flatMap(s => s.split(",").toSeq)
        .contains(classOf[QueryExecutionEventHandler].getCanonicalName)
      if (!splineConfiguredForCodelessInit || spark.SPARK_VERSION.startsWith("2.2")) {
        if (splineConfiguredForCodelessInit) {
          log.warn(
            """
              |Spline lineage tracking is also configured for codeless initialization, but codeless init is
              |supported on Spark 2.3+ and not current version 2.2. Spline will be initialized only via code call to
              |enableLineageTracking i.e. the same way as is now."""
              .stripMargin.replaceAll("\n", " "))
        }


        new QueryExecutionEventHandlerFactory(sparkSession)
          .createEventHandler(configurer)
          .foreach(eventHandler =>
            sparkSession.listenerManager.register(new SplineQueryExecutionListener(Some(eventHandler))))

      } else {
        log.warn(
          """
            |Spline lineage tracking is also configured for codeless initialization.
            |It wont be initialized by this code call to enableLineageTracking now."""
            .stripMargin.replaceAll("\n", " "))
      }
      sparkSession
    }
  }

  val InitFlagKey = "spline.initialized_flag"

  val SparkQueryExecutionListenersKey = "spark.sql.queryExecutionListeners"
}
