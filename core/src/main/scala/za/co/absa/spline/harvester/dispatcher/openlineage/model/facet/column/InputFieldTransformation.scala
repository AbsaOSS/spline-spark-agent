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

package za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.column

case class InputFieldTransformation(
  /* The type of the transformation. Allowed values are: DIRECT, INDIRECT */
  `type`: String,
  /* The subtype of the transformation */
  subtype: Option[String] = None,
  /* a string representation of the transformation applied */
  description: Option[String] = None,
  /* is transformation masking the data or not */
  masking: Option[Boolean] = None
)
