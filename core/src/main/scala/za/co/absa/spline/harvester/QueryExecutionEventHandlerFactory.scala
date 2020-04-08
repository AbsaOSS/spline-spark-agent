/*
 * Copyright 2020 ABSA Group Limited
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
import za.co.absa.commons.buildinfo.BuildInfo
import za.co.absa.spline.harvester.SparkLineageInitializer.InitFlagKey
import za.co.absa.spline.harvester.conf.SplineConfigurer
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode._

import scala.util.control.NonFatal

class QueryExecutionEventHandlerFactory(sparkSession: SparkSession) extends Logging {

  def createEventHandler(configurer: SplineConfigurer): Option[QueryExecutionEventHandler] = {
    if (configurer.splineMode != DISABLED) {
      if (!getOrSetIsInitialized()) {
        log.info(s"Initializing Spline agent...")
        log.info(s"Spline version: ${BuildInfo.Version}")
        log.info(s"Spline mode: ${configurer.splineMode}")

        try {
          val eventHandler = configurer.queryExecutionEventHandler
          log.info(s"Spline successfully initialized. Spark Lineage tracking is ENABLED.")
          Some(eventHandler)

        } catch {
          case NonFatal(e) if configurer.splineMode == BEST_EFFORT =>
            log.error(s"Spline initialization failed! Spark Lineage tracking is DISABLED.", e)
            None
        }
      } else {
        log.warn("Spline lineage tracking is already initialized!")
        None
      }
    } else {
      None
    }
  }

  private def getOrSetIsInitialized(): Boolean = sparkSession.synchronized {
    val sessionConf = sparkSession.conf
    sessionConf getOption InitFlagKey match {
      case Some(_) =>
        true
      case None =>
        sessionConf.set(InitFlagKey, true.toString)
        false
    }
  }

}
