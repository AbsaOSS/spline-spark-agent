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

package za.co.absa.spline.harvester.conf

import za.co.absa.spline.harvester.QueryExecutionEventHandler

/**
  * The trait describes settings needed for initialization of the library.
  */
trait SplineConfigurer {

  import SplineConfigurer.SplineMode._
  import SplineConfigurer.SQLFailureCaptureMode._

  /**
    * A listener handling events from batch processing
    * @return [[za.co.absa.spline.harvester.QueryExecutionEventHandler]]
    */
  def queryExecutionEventHandler: QueryExecutionEventHandler

  /**
    * Spline mode designates how Spline should behave in a context of a Spark application.
    * It mostly relates to error handling. E.g. is lineage tracking a mandatory for the given Spark app or is it good to have.
    * Should the Spark app be aborted on Spline errors or not.
    *
    * @see [[SplineMode]]
    * @return [[SplineMode]]
    */
  def splineMode: SplineMode

  /**
   * Controls if failed SQL execution should be captured depending on the kind of associated error
   * @return [[SQLFailureCaptureMode]]
   */
  def sqlFailureCaptureMode: SQLFailureCaptureMode
}

object SplineConfigurer {

  object SplineMode extends Enumeration {
    type SplineMode = Value
    val

    // Spline is disabled completely
    DISABLED,

    // Abort on Spline initialization errors
    REQUIRED,

    // If Spline initialization fails then disable Spline and continue without lineage tracking
    BEST_EFFORT

    = Value
  }

  object SQLFailureCaptureMode extends Enumeration {
    type SQLFailureCaptureMode = Value
    val

    // Do NOT capture any failed SQL executions
    NONE,

    // Only capture failed SQL executions when the error is non-fatal (see [[scala.util.control.NonFatal]])
    NON_FATAL,

    // Capture all failed SQL executions regardless of the error type
    ALL

    = Value
  }

}
