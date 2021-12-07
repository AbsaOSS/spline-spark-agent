/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher.openlindispatcher.model.facet

import za.co.absa.spline.producer.model.openlineage.v0_3_1.RunFacet

class SplineLineageFacet(
  override val _producer: String,
  override val _schemaURL: String,
  val executionPlan: AnyRef,
  val executionEvent: AnyRef,
  val apiVersion: String
) extends RunFacet(_producer, _schemaURL)
