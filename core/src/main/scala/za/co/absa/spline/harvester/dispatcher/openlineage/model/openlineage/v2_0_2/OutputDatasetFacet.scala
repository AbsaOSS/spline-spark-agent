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

package za.co.absa.spline.harvester.dispatcher.openlineage.model.openlineage.v2_0_2

/**
 * An Output Dataset Facet
 *
 * @param _producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha for example: '''https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client'''
 * @param _schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet for example: '''https://openlineage.io/spec/1-0-2/OpenLineage.json#/$defs/BaseFacet'''
 */
case class OutputDatasetFacet (
  _producer: String,
  _schemaURL: String
)
