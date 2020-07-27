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

package za.co.absa.spline.harvester.plugin.impl

import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.impl.CobrixPlugin.`_: CobolRelation`
import za.co.absa.spline.harvester.plugin.{BaseRelationPlugin, Plugin}


class CobrixPlugin extends Plugin with BaseRelationPlugin {

  override def baseRelProcessor: PartialFunction[(BaseRelation, LogicalRelation), (SourceIdentifier, Params)] = {

    case (`_: CobolRelation`(cr), _) =>
      val sourceDir = extractFieldValue[String](cr, "sourceDir")
      (SourceIdentifier.forCobol(sourceDir), Map.empty)
  }
}


object CobrixPlugin {

  object `_: CobolRelation` extends SafeTypeMatchingExtractor[AnyRef]("za.co.absa.cobrix.spark.cobol.source.CobolRelation")

}
