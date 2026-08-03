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
 * @param eventTime the time the event occurred at
 * @param producer URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha for example: '''https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client'''
 * @param schemaURL The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent for example: '''https://openlineage.io/spec/0-0-1/OpenLineage.json'''
 * @param eventType the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete for example: '''START|RUNNING|COMPLETE|ABORT|FAIL|OTHER'''
 * @param run 
 * @param job 
 * @param inputs The set of **input** datasets.
 * @param outputs The set of **output** datasets.
 */
case class RunEvent (
  eventTime: String,
  producer: String,
  schemaURL: String,
  eventType: Option[String],
  run: Run,
  job: Job,
  inputs: Option[Seq[InputDataset]],
  outputs: Option[Seq[OutputDataset]]
)
