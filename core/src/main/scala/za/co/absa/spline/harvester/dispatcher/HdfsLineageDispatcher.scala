/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.Configuration
import org.apache.hadoop.conf
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import za.co.absa.commons.lang.ARM
import za.co.absa.spline.harvester
import za.co.absa.spline.harvester.dispatcher.hdfsdispatcher.naming.NamingStrategy
import za.co.absa.spline.harvester.dispatcher.hdfsdispatcher.path.PathStrategy
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import scala.concurrent.blocking
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

class HdfsLineageDispatcher(configuration: Configuration) extends LineageDispatcher with Logging {

  private val hadoopConfiguration: conf.Configuration = SparkContext.getOrCreate().hadoopConfiguration
  private val baseFileName: String = configuration getString(HdfsLineageDispatcher.fileNameKey, "_SPLINE_LINEAGE")
  private val defaultFilePermissions: FsPermission = FsPermission.getFileDefault.applyUMask(
    FsPermission.getUMask(FileSystem.get(hadoopConfiguration).getConf))
  private val filePermissions = new FsPermission(configuration.getString(
    HdfsLineageDispatcher.filePermissionsKey,
    defaultFilePermissions.toShort.toString))
  private val namingStrategy: NamingStrategy = harvester.instantiate[NamingStrategy](
    configuration,
    configuration.getString(
      HdfsLineageDispatcher.namingStrategyKey,
      "za.co.absa.spline.harvester.dispatcher.hdfsdispatcher.naming.DefaultNamingStrategy"))
  private val pathStrategy: PathStrategy = harvester.instantiate[PathStrategy](
    configuration,
    configuration.getString(
      HdfsLineageDispatcher.pathStrategyKey,
      "za.co.absa.spline.harvester.dispatcher.hdfsdispatcher.path.DefaultPathStrategy"))

  override def send(executionPlan: ExecutionPlan): String = {
    val maybeBasePath: Option[String] = getPath(executionPlan)

    maybeBasePath.map(_.trim).filter(_.nonEmpty) match {
      case Some(path) =>
        val content: String = executionPlan.toJson
        val fileName: String = namingStrategy(executionPlan,
                                              baseFileName)
        val fullPath: Path = new Path(path, fileName)
        Try(persistToHdfs(content, fullPath)) match {
          case Success(_) => log.info(s"Persisted execution plan ${executionPlan.id} to $fullPath")
          case Failure(t) => persistFailed(executionPlan,
                                           fullPath,
                                           t)
        }
      case None => log.error(s"No path available to persist execution plan ${executionPlan.id}")
    }
    executionPlan.id.toJson
  }

  override def send(event: ExecutionEvent): Unit = {
    // no operation
  }

  private def persistToHdfs(content: String, path: Path): Unit = blocking {
    val fs: FileSystem = FileSystem.get(hadoopConfiguration)
    log debug s"Writing lineage to $path"
    ARM.using(fs.create(
      path,
      filePermissions,
      true,
      hadoopConfiguration.getInt("io.file.buffer.size", 4096),
      fs.getDefaultReplication(path),
      fs.getDefaultBlockSize(path),
      null)) {
      _.write(content.getBytes)
    }
  }

  /**
   * Get the path to persist the data to.
   *
   * @param executionPlan the execution plan
   * @return an Option of the path
   */
  def getPath(executionPlan: ExecutionPlan): Option[String] = pathStrategy(executionPlan)

  /**
   * Invoked when writing to HDFS fails.
   *
   * @param executionPlan the execution plan
   * @param path the HDFS path
   * @param t the [[Throwable]] given by the failure
   */
  def persistFailed(executionPlan: ExecutionPlan,
                    path: Path,
                    t: Throwable): Unit = log.error(s"Failed to persist execution plan ${executionPlan.id} to $path",
                                                    t)
}

object HdfsLineageDispatcher {
  val fileNameKey = "spline.hdfs.file.name"
  val filePermissionsKey = "spline.hdfs.file.permissions"
  val namingStrategyKey = "spline.hdfs.naming.strategy"
  val pathStrategyKey = "spline.hdfs.path.strategy"
}
