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

package za.co.absa.spline.harvester.plugin.embedded

import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.XMLPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import javax.annotation.Priority

@Priority(Precedence.Normal)
class XMLPlugin(pathQualifier: PathQualifier) extends Plugin with BaseRelationProcessing {

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: XmlRelation`(xr), _) =>
      val parameters = extractValue[Map[String, String]](xr, "parameters")
      val location = extractValue[Option[String]](xr, "location")
      val qualifiedPaths = location.toSeq.map(pathQualifier.qualify)
      ReadNodeInfo(asSourceId(qualifiedPaths), parameters)
  }
}

object XMLPlugin {

  private object `_: XmlRelation` extends SafeTypeMatchingExtractor[AnyRef]("com.databricks.spark.xml.XmlRelation")

  private def asSourceId(paths: Seq[String]) = SourceIdentifier(Some("xml"), paths: _*)
}
