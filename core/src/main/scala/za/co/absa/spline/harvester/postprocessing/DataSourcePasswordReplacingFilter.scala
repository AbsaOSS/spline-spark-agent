/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.postprocessing

import org.apache.commons.configuration.Configuration
import za.co.absa.commons.CaptureGroupReplacer
import za.co.absa.commons.config.ConfigurationImplicits.ConfigurationRequiredWrapper
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.harvester.postprocessing.DataSourcePasswordReplacingFilter.{RegexesKey, ReplacementKey}
import za.co.absa.spline.producer.model.v1_1.{ReadOperation, WriteOperation}

import scala.util.matching.Regex

class DataSourcePasswordReplacingFilter(replacement: String, regexes: Seq[Regex])
  extends AbstractPostProcessingFilter {

  def this(conf: Configuration) = this(
    conf.getRequiredString(ReplacementKey),
    conf.getRequiredStringArray(RegexesKey).map(_.r)
  )

  override def processReadOperation(op: ReadOperation, ctx: HarvestingContext): ReadOperation =
    op.copy(inputSources = op.inputSources.map(filter))

  override def processWriteOperation(op: WriteOperation, ctx: HarvestingContext): WriteOperation =
    op.copy(outputSource = filter(op.outputSource))

  private val replacer = new CaptureGroupReplacer(replacement)

  private def filter(uri: String): String =
    replacer.replace(uri, regexes)
}

object DataSourcePasswordReplacingFilter {
  final val ReplacementKey = "replacement"
  final val RegexesKey = "regexes"
}
