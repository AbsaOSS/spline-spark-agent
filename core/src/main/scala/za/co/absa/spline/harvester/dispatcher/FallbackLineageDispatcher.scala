/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher

import org.apache.spark.internal.Logging
import za.co.absa.spline.HierarchicalObjectFactory
import za.co.absa.spline.harvester.dispatcher.FallbackLineageDispatcher.{FallbackDispatcherKey, PrimaryDispatcherKey}
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import scala.util.control.NonFatal

class FallbackLineageDispatcher(primaryDispatcher: LineageDispatcher, fallbackDispatcher: LineageDispatcher)
  extends LineageDispatcher with Logging {

  def this(objectFactory: HierarchicalObjectFactory) = this(
    objectFactory.createComponentByKey(PrimaryDispatcherKey),
    objectFactory.createComponentByKey(FallbackDispatcherKey)
  )

  logInfo("Fallback dispatcher configured:")
  logInfo(s"primaryDispatcher=${primaryDispatcher.getClass.getName}")
  logInfo(s"fallbackDispatcher=${fallbackDispatcher.getClass.getName}")


  override def send(plan: ExecutionPlan): Unit =
    try primaryDispatcher.send(plan)
    catch {
      case e if NonFatal(e) =>
        logError("Error when sending ExecutionPlan, will try to use fallback dispatcher next", e)
        fallbackDispatcher.send(plan)
    }

  override def send(event: ExecutionEvent): Unit =
    try primaryDispatcher.send(event)
    catch {
      case e if NonFatal(e) =>
        logError("Error when sending ExecutionEvent, will try to use fallback dispatcher next", e)
        fallbackDispatcher.send(event)
    }
}

object FallbackLineageDispatcher {
  private val PrimaryDispatcherKey = "primaryDispatcher"
  private val FallbackDispatcherKey = "fallbackDispatcher"
}
