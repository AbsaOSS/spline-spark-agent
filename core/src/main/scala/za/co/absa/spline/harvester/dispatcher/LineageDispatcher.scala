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

package za.co.absa.spline.harvester.dispatcher

import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

/**
 * <p>
 * This trait deals with the captured lineage information.
 * When the lineage data is ready the instance of this trait is called to publish the result.
 * Which implementation is used depends on the configuration.
 * <br>
 * See [[za.co.absa.spline.harvester.conf.SplineConfigurer#lineageDispatcher]]
 * </p>
 * <br>
 * <p>
 * If you are using default Spline configurer a custom lineage dispatcher can be registered via the configuration property
 * [[za.co.absa.spline.harvester.conf.DefaultSplineConfigurer.ConfProperty#LINEAGE_DISPATCHER_CLASS]]
 * <br>
 * See: [[za.co.absa.spline.harvester.conf.StandardSplineConfigurationStack]]
 * </p>
 * <br>
 * <p>
 * When registering the class via a property (using the Default Spline Configurer)
 * the class has to have a constructor with the following signature:
 * {{{
 *    @throws[SplineInitializationException]
 *    def constr(conf: org.apache.commons.configuration.Configuration)
 * }}}
 * </p>
 *
 *
 */
@throws[SplineInitializationException]
trait LineageDispatcher {

  def send(executionPlan: ExecutionPlan): String

  def send(event: ExecutionEvent): Unit
}
