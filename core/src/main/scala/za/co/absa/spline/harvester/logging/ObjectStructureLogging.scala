/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.logging

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait ObjectStructureLogging {
  self: Logging =>

  protected def logTraceObjectStructure(obj: => AnyRef): Unit =
    if (log.isTraceEnabled) logObjectStructure(obj, log.trace, log.trace)

  protected def logWarningObjectStructure(obj: => AnyRef): Unit =
    if (log.isWarnEnabled) logObjectStructure(obj, log.warn, log.warn)

  protected def logErrorObjectStructure(obj: => AnyRef): Unit =
    if (log.isErrorEnabled) logObjectStructure(obj, log.error, log.error)

  private def logObjectStructure(
    obj: AnyRef,
    logMethod: String => Unit,
    logMethodThrowable: (String, Throwable) => Unit
  ): Unit =
    Try(ObjectStructureDumper.dump(obj)) match {
      case Success(s) => logMethod(s)
      case Failure(e) if NonFatal(e) => logMethodThrowable(s"Attempt to dump structure of ${obj.getClass} failed", e)
    }
}
