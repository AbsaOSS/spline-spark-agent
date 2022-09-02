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

import org.apache.spark.Partition
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileScanRDD
import za.co.absa.commons.reflect.ReflectionUtils
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.plugin.Plugin.{Params, Precedence}
import za.co.absa.spline.harvester.plugin.{Plugin, RddReadNodeProcessing}
import za.co.absa.spline.harvester.qualifier.PathQualifier

import java.net.URI
import javax.annotation.Priority
import scala.language.reflectiveCalls

@Priority(Precedence.Normal)
class RDDPlugin(
  pathQualifier: PathQualifier,
  session: SparkSession)
  extends Plugin
    with RddReadNodeProcessing  {

  override def rddReadNodeProcessor: PartialFunction[RDD[_], (SourceIdentifier, Params)] = {
    case fsr: FileScanRDD =>
      val uris = fsr.filePartitions.flatMap(_.files.map(_.filePath))
      (SourceIdentifier(None, uris: _*), Map.empty)
    case hr: HadoopRDD[_, _]  =>
      val partitions = ReflectionUtils.extractValue[Array[Partition]](hr, "partitions_")
      val uris = partitions.map(hadoopPartitionToUriString)
      (SourceIdentifier(None, uris: _*), Map.empty)
  }

  private def hadoopPartitionToUriString(hadoopPartition: Partition): String = {
    val inputSplit = ReflectionUtils.extractValue[AnyRef](hadoopPartition, "inputSplit")
    val fileSplitT = ReflectionUtils.extractValue[AnyRef](inputSplit, "t")
    val fileSplitFs = ReflectionUtils.extractValue[AnyRef](fileSplitT, "fs")
    val file = ReflectionUtils.extractValue[AnyRef](fileSplitFs, "file")
    val uri = ReflectionUtils.extractValue[URI](file, "uri")

    uri.toString
  }

}
