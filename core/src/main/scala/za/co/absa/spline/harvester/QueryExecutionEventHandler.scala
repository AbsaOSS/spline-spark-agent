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

package za.co.absa.spline.harvester

import org.apache.spark.sql.execution.QueryExecution
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class QueryExecutionEventHandler(
  harvesterFactory: LineageHarvesterFactory,
  lineageDispatcher: LineageDispatcher) {

  def handle(qe: QueryExecution, result: Try[Duration]): Unit = {
    harvesterFactory
      .harvester(qe.analyzed, Some(qe.executedPlan))
      .harvest(result)
      .foreach({
        case (plan, event) =>
          lineageDispatcher.send(plan)
          lineageDispatcher.send(event)
      })
  }
}
