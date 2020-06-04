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

import org.apache.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.spline.harvester.SparkLineageInitializer.{InitFlagKey, SparkQueryExecutionListenersKey}
import za.co.absa.spline.harvester.conf.SplineConfigurer
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode._
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener

import scala.util.control.NonFatal

class QueryExecutionEventHandlerFactory(sparkSession: SparkSession) extends Logging {

  def createEventHandler(configurer: SplineConfigurer, isCodelessInit: Boolean): Option[QueryExecutionEventHandler] = {
    log.info(s"Initializing Spline agent...")
    log.info(s"Spline version: ${SplineBuildInfo.Version}")
    log.info(s"Spline mode: ${configurer.splineMode}")
    log.info(s"""Spline initialization type: ${if (isCodelessInit) "codeless" else "manual"}""")

    if (configurer.splineMode == DISABLED) {
      log.info("initialization aborted")
      None
    } else {
      withErrorHandling(configurer) {
        if (isCodelessInit) {
          Some(initEventHandler(configurer))
        } else {
          assureOneListenerPerSession(configurer) {
            initEventHandler(configurer)
          }
        }
      }
    }
  }

  private def withErrorHandling(configurer: SplineConfigurer)(body: => Option[QueryExecutionEventHandler]) = {
    try {
      body
    } catch {
      case NonFatal(e) if configurer.splineMode == BEST_EFFORT =>
        log.error(s"Spline initialization failed! Spark Lineage tracking is DISABLED.", e)
        None
    }
  }

  private def initEventHandler(configurer: SplineConfigurer): QueryExecutionEventHandler = {
    val eventHandler = configurer.queryExecutionEventHandler
    log.info(s"Spline successfully initialized. Spark Lineage tracking is ENABLED.")
    eventHandler
  }

  private def assureOneListenerPerSession(configurer: SplineConfigurer)(body: => QueryExecutionEventHandler) = {
    if (isCodelessInitActive()) {
      None
    } else {
      if (getOrSetIsInitialized()) {
        log.warn("Spline lineage tracking is already initialized!")
        None
      } else {
        try {
          Some(body)
        } catch {
          case NonFatal(e) =>
            setUninitialized()
            throw e
        }
      }
    }
  }

  private def isCodelessInitActive(): Boolean = {
    val splineConfiguredForCodelessInit = sparkSession.sparkContext.getConf
      .getOption(SparkQueryExecutionListenersKey).toSeq
      .flatMap(s => s.split(",").toSeq)
      .contains(classOf[SplineQueryExecutionListener].getCanonicalName)

    if (!splineConfiguredForCodelessInit) {
      false
    } else if (spark.SPARK_VERSION.startsWith("2.2")) {
      log.warn(
        """
          |Spline lineage tracking is also configured for codeless initialization, but codeless init is
          |supported on Spark 2.3+ and not current version 2.2. Spline will be initialized only via code call to
          |enableLineageTracking i.e. the same way as is now."""
          .stripMargin.replaceAll("\n", " ").trim())
      false
    } else {
      log.warn(
        """
          |Spline lineage tracking is also configured for codeless initialization.
          |It wont be initialized by this code call to enableLineageTracking now."""
          .stripMargin.replaceAll("\n", " ").trim())
      true
    }
  }

  private def getOrSetIsInitialized(): Boolean = sparkSession.synchronized {
    val sessionConf = sparkSession.conf
    sessionConf getOption InitFlagKey match {
      case Some("true") =>
        true
      case _ =>
        sessionConf.set(InitFlagKey, true.toString)
        false
    }
  }

  private def setUninitialized(): Unit = sparkSession.synchronized {
    sparkSession.conf.set(InitFlagKey, false.toString)
  }

}
