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

package za.co.absa.spline.harvester.listener.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import za.co.absa.spline.harvester.SparkStreamingLineageInitializer

class SplineStreamingQueryListener extends StreamingQueryListener {

  private val maybeListener: Option[StreamingQueryListener] = {
    val sparkSession = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .getOrElse(throw new IllegalStateException("Session is unexpectedly missing. Spline cannot be initialized."))

    new SparkStreamingLineageInitializer(sparkSession).createListener(isCodelessInit = true)
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    maybeListener.foreach(_.onQueryStarted(event))

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit =
    maybeListener.foreach(_.onQueryProgress(event))

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
    maybeListener.foreach(_.onQueryTerminated(event))

}
