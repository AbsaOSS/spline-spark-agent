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
 * @param namespace The namespace containing that dataset for example: '''my-datasource-namespace'''
 * @param name The unique name for that dataset within that namespace for example: '''instance.schema.table'''
 * @param facets The facets for this dataset
 */
case class Dataset (
  namespace: String,
  name: String,
  facets: Option[Map[String, DatasetFacet]]
)
