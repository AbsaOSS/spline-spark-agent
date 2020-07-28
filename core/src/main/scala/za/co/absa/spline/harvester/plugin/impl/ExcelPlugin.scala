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

import java.io.InputStream

import com.crealytics.spark.excel.{DefaultSource, ExcelRelation, WorkbookReader}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.Params
import za.co.absa.spline.harvester.plugin.impl.ExcelPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationPlugin, DataSourceFormatPlugin, Plugin}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.util.Try


class ExcelPlugin(pathQualifier: PathQualifier)
  extends Plugin
    with BaseRelationPlugin
    with DataSourceFormatPlugin {

  override def baseRelProcessor: PartialFunction[(BaseRelation, LogicalRelation), (SourceIdentifier, Params)] = {
    case (`_: ExcelRelation`(exr), _) =>
      val excelRelation = exr.asInstanceOf[ExcelRelation]
      val inputStream = extractExcelInputStream(excelRelation.workbookReader)
      val path = extractFieldValue[org.apache.hadoop.fs.Path](inputStream, "file")
      val qualifiedPath = pathQualifier.qualify(path.toString)
      val sourceId = asSourceId(qualifiedPath)
      val params = extractExcelParams(excelRelation) + ("header" -> excelRelation.header.toString)
      (sourceId, params)
  }

  override def formatNameResolver: PartialFunction[AnyRef, String] = {
    case "com.crealytics.spark.excel" | `_: excel.DefaultSource`(_) => "excel"
  }
}

object ExcelPlugin {

  private object `_: ExcelRelation` extends SafeTypeMatchingExtractor[AnyRef]("com.crealytics.spark.excel.ExcelRelation")

  private object `_: excel.DefaultSource` extends SafeTypeMatchingExtractor(classOf[DefaultSource])

  private def extractExcelInputStream(reader: WorkbookReader) = {

    val streamFieldName_Scala_2_12 = "inputStreamProvider"
    val streamFieldName_Scala_2_11_default = "com$crealytics$spark$excel$DefaultWorkbookReader$$inputStreamProvider"
    val streamFieldName_Scala_2_11_streaming = "com$crealytics$spark$excel$StreamingWorkbookReader$$inputStreamProvider"

    def extract(fieldName: String) = extractFieldValue[() => InputStream](reader, fieldName)

    val lazyStream = Try(extract(streamFieldName_Scala_2_12))
      .orElse(Try(extract(streamFieldName_Scala_2_11_default)))
      .orElse(Try(extract(streamFieldName_Scala_2_11_streaming)))
      .getOrElse(sys.error("Unable to extract Excel input stream"))

    lazyStream.apply()
  }

  private def extractExcelParams(excelRelation: ExcelRelation): Map[String, Any] = {
    val locator = excelRelation.dataLocator

    def extract(fieldName: String) =
      Try(extractFieldValue[Any](locator, fieldName))
        .map(_.toString)
        .getOrElse("")

    val fieldNames = locator.getClass.getDeclaredFields.map(_.getName)

    fieldNames.map(name => name -> extract(name)).toMap
  }

  private def asSourceId(filePath: String) = SourceIdentifier(Some("excel"), filePath)
}
