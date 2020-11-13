/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener.constructEventHandler
import za.co.absa.spline.harvester.{QueryExecutionEventHandler, QueryExecutionEventHandlerFactory}

import scala.util.control.NonFatal

class SplineQueryExecutionListener(maybeEventHandler: Option[QueryExecutionEventHandler])
  extends QueryExecutionListener
    with Logging {

  /**
   * Listener delegate is lazily evaluated as Spline initialization requires completely initialized SparkSession
   * to be able to use sessionState for duplicate tracking prevention.
   */
  def this() = this(constructEventHandler())

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = withErrorHandling(qe) {
    maybeEventHandler.foreach(_.onSuccess(funcName, qe, durationNs))
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = withErrorHandling(qe) {
    maybeEventHandler.foreach(_.onFailure(funcName, qe, exception))
  }

  private def withErrorHandling(qe: QueryExecution)(body: => Unit): Unit = {
    try
      body
    catch {
      case NonFatal(e) =>
        val ctx = qe.sparkSession.sparkContext
        logError(s"Unexpected error occurred during lineage processing for application: ${ctx.appName} #${ctx.applicationId}", e)
    }
  }
}

object SplineQueryExecutionListener extends Logging {

  private def constructEventHandler(): Option[QueryExecutionEventHandler] = {
    val sparkSession = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse(throw new IllegalStateException("Session is unexpectedly missing. Spline cannot be initialized."))

    val configurer = DefaultSplineConfigurer(sparkSession)

    new QueryExecutionEventHandlerFactory(sparkSession).createEventHandler(configurer, true)
  }
}
