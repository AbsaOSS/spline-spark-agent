/*
 * Copyright 2019 ABSA Group Limited
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
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import scala.util.{Failure, Success, Try}

/**
 * Dispatch execution plans and events to multiple targets.
 *
 * A primary delegate is specified to drive the link between plans and events.
 * Any events returned from secondary dispatchers are discarded.
 *
 * @param primaryDelegate the primary lineage dispatcher
 * @param maybeSecondaryDelegates any other dispatchers
 */
class AggregateLineageDispatcher(val primaryDelegate: LineageDispatcher,
                                 val maybeSecondaryDelegates: Option[Seq[LineageDispatcher]] = None)
  extends LineageDispatcher
  with Logging {

  /**
   * Dispatch the execution plan using all dispatchers.  The return value of the primary
   * dispatcher is the return value of this function.
   *
   * @param executionPlan the execution plan to dispatch
   * @return the return value of the primary dispatcher
   */
  override def send(executionPlan: ExecutionPlan): String = {
    def dispatchPlan(dispatcher: LineageDispatcher): String = {
      Try(
        dispatcher.send(executionPlan)
      )
      match {
        case Success(result) =>
          log.info(s"Dispatched plan to ${name(dispatcher)} with result [$result]")
          result
        case Failure(t) =>
          log.error(s"Failed to dispatch plan to ${name(dispatcher)} because [${t.getMessage}]",
                    t)
          "error"
      }
    }
    maybeSecondaryDelegates.foreach(dispatchers => dispatchers.foreach(dispatcher => dispatchPlan(dispatcher)))
    dispatchPlan(primaryDelegate)
  }

  /**
   * Dispatch the execution event using all dispatchers.
   *
   * @param event the execution event to dispatch
   */
  override def send(event: ExecutionEvent): Unit = {
    def dispatchEvent(dispatcher: LineageDispatcher): Unit = {
      Try(
        dispatcher.send(event)
      )
      match {
        case Success(_) => log.info(s"Dispatched event to ${name(dispatcher)}")
        case Failure(t) => log.error(s"Failed to dispatch event to ${name(dispatcher)} because [${t.getMessage}]", t)
      }
    }

    maybeSecondaryDelegates.foreach(dispatchers => dispatchers.foreach(dispatcher => dispatchEvent(dispatcher)))
    dispatchEvent(primaryDelegate)
  }

  private def name(o: Any): String = o.getClass.getName
}
