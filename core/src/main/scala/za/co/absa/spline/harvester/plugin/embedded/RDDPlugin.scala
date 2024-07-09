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
import org.apache.spark.sql.execution.datasources.{FileScanRDD, PartitionedFile}
import za.co.absa.spline.commons.reflect.ReflectionUtils
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo}
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
    with RddReadNodeProcessing {

  override def rddReadNodeProcessor: PartialFunction[RDD[_], ReadNodeInfo] = {
    case fsr: FileScanRDD =>
      val files = fsr.filePartitions.flatMap(_.files)
      val uris = files.map(extractPath(_))
      ReadNodeInfo(SourceIdentifier(None, uris: _*), Map.empty)
    case hr: HadoopRDD[_, _] =>
      val partitions = ReflectionUtils.extractValue[Array[Partition]](hr, "partitions_")
      val uris = partitions.map(hadoopPartitionToUriString)
      ReadNodeInfo(SourceIdentifier(None, uris: _*), Map.empty)
  }

  private def extractPath(file: PartitionedFile): String = {
    val path = ReflectionUtils.extractValue[AnyRef](file, "filePath")
    // for Spark 3.3 and lower path is a String
    // for Spark 3.4 path is org.apache.spark.paths.SparkPath
    path.toString
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
