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

import za.co.absa.spline.harvester.dispatcher.openlineage.model.openlineage.v2_0_2.DatasetFacet

case class ColumnLineageDatasetFacet(
  /* URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha */
  _producer: String,
  /* The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet */
  _schemaURL: String,
  /* set to true to delete a facet */
  _deleted: Option[Boolean] = None,
  /* Column level lineage that maps output fields into input fields used to evaluate them. */
  fields: Map[String, ColumnLineage],
  /* Column level lineage that affects the whole dataset. This includes filtering, sorting, grouping (aggregates), joining, window functions, etc. */
  dataset: Option[Seq[InputField]] = None
) extends DatasetFacet(_producer, _schemaURL, _deleted)
