/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import za.co.absa.spline.commons.annotation.Experimental
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.commons.lang.ARM._
import za.co.absa.spline.commons.s3.SimpleS3Location.SimpleS3LocationExt
import za.co.absa.spline.harvester.dispatcher.HDFSLineageDispatcher._
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.blocking

@Experimental
class HDFSLineageDispatcher(filename: String, permission: FsPermission, bufferSize: Int)
  extends LineageDispatcher
    with Logging {

  def this(conf: Configuration) = this(
    filename = conf.getRequiredString(FileNameKey),
    permission = new FsPermission(conf.getRequiredObject(FilePermissionsKey).toString),
    bufferSize = conf.getRequiredInt(BufferSizeKey)
  )

  @volatile
  private var _lastSeenPlan: ExecutionPlan = _

  override def name = "HDFS"

  override def send(plan: ExecutionPlan): Unit = {
    this._lastSeenPlan = plan
  }

  override def send(event: ExecutionEvent): Unit = {
    if (this._lastSeenPlan == null || this._lastSeenPlan.id.get != event.planId)
      throw new IllegalStateException("send(event) must be called strictly after send(plan) method with matching plan ID")

    try {
      val sparkContext = SparkContext.getOrCreate()

      val lineageBaseDir = sparkContext.getConf.get(
        "spark.spline.lineageDispatcher.hdfs.directory",
        "file:///C:/tmp" // Default to local storage on Windows
      )

      val executionPlanID = this._lastSeenPlan.id.getOrElse("unknown_plan")
      val executionPlanName = this._lastSeenPlan.name.replaceAll("[^a-zA-Z0-9_\\-]", "_")
      val runID = event.extra.get("appId").map(_.toString).getOrElse("unknown_runID")

      val appDir = s"$lineageBaseDir/$executionPlanName"
      val runDir = s"$appDir/$runID"

      // Best-effort cleanup of previous runs
      cleanupOldRunFolders(appDir, runID)

      val fileName = s"lineage_${executionPlanID}.json"

      // Build JSON payload
      val planWithEvent = Map(
        "executionPlan" -> this._lastSeenPlan,
        "executionEvent" -> event
      )
      import HarvesterJsonSerDe.impl._
      val content = planWithEvent.toJson

      // Atomically materialize the run folder & file
      persistRunFolderAtomically(runDir, fileName, content)
    } finally {
      this._lastSeenPlan = null
    }
  }

  /**
   * Deletes all runID folders except the current one
   * @param appDirPath Path to the app directory
   * @param currentRunID The current runID to keep
   */
  private def cleanupOldRunFolders(appDirPath: String, currentRunID: String): Unit = {
    try {
      val (fs, appPath) = pathStringToFsWithPath(appDirPath)

      if (fs.exists(appPath) && fs.getFileStatus(appPath).isDirectory) {
        val statuses = fs.listStatus(appPath)
        statuses.foreach { status =>
          if (status.isDirectory && status.getPath.getName != currentRunID) {
            logInfo(s"Deleting old runID folder: ${status.getPath}")
            fs.delete(status.getPath, true) // recursive delete
          }
        }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to cleanup old run folders in $appDirPath", e)
    }
  }

  /**
   * Atomically create a run directory with its lineage JSON file inside.
   * Strategy:
   *  1) Ensure parent exists (mkdirs is idempotent)
   *  2) Create a hidden temp directory next to the target
   *  3) Write JSON to a temp file, then rename temp file -> final name inside temp dir
   *  4) Rename temp dir -> final run dir (atomic on HDFS)
   */
  private def persistRunFolderAtomically(finalRunDirStr: String, fileName: String, content: String): Unit = blocking {
    val (fs, finalRunDir) = pathStringToFsWithPath(finalRunDirStr)

    // Ensure final run directory exists
    if (!fs.exists(finalRunDir)) {
      fs.mkdirs(finalRunDir)
      try fs.setPermission(finalRunDir, permission) catch { case _: Throwable => () }
    }

    // Write the file atomically inside the run directory
    val finalFileInRunDir = new Path(finalRunDir, fileName)
    writeFileAtomically(fs, finalFileInRunDir, content.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Write a single file atomically via temp-sibling + rename.
   */
  private def writeFileAtomically(fs: FileSystem, finalPath: Path, bytes: Array[Byte]): Unit = {
    val parent = finalPath.getParent
    if (!fs.exists(parent)) {
      fs.mkdirs(parent)
    }

    val tmpFile = new Path(parent, s".${finalPath.getName}.tmp-${UUID.randomUUID().toString}")
    val replication = fs.getDefaultReplication(finalPath)
    val blockSize = fs.getDefaultBlockSize(finalPath)

    logDebug(s"Creating temp file $tmpFile")
    val out = fs.create(tmpFile, permission, true, bufferSize, replication, blockSize, null)
    try {
      out.write(bytes)
      // Best-effort durability hints; on HDFS these are meaningful.
      out.hflush()
      out.hsync()
    } finally {
      out.close()
    }

    try fs.setPermission(tmpFile, permission) catch { case _: Throwable => () }

    logDebug(s"Renaming $tmpFile -> $finalPath (atomic on HDFS)")
    if (!fs.rename(tmpFile, finalPath)) {
      try fs.delete(tmpFile, false) catch { case _: Throwable => () }
      throw new RuntimeException(s"Failed to atomically rename $tmpFile to $finalPath")
    }
  }

  // Kept for compatibility: atomic-at-file-level write for a direct full path
  private def persistToHadoopFs(content: String, fullLineagePath: String): Unit = blocking {
    val (fs, path) = pathStringToFsWithPath(fullLineagePath)
    val parentDir = path.getParent
    if (!fs.exists(parentDir)) {
      fs.mkdirs(parentDir)
    }
    writeFileAtomically(fs, path, content.getBytes(StandardCharsets.UTF_8))
  }
}

object HDFSLineageDispatcher {
  private val HadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration

  private val FileNameKey = "fileName"
  private val FilePermissionsKey = "filePermissions"
  private val BufferSizeKey = "fileBufferSize"

  /**
   * Converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `file:///path/to/file` -> local FS + `/path/to/file`
   * `/path/on/hdfs/to/file` -> HDFS + `/path/on/hdfs/to/file`
   *
   * @param pathString path to convert to FS and relative path
   * @return FS + relative path
   */
  def pathStringToFsWithPath(pathString: String): (FileSystem, Path) = {
    pathString.toSimpleS3Location match {
      case Some(s3Location) =>
        val s3Uri = new URI(s3Location.asSimpleS3LocationString) // s3://<bucket>
        val s3Path = new Path(s"/${s3Location.path}")            // /<text-file-object-path>
        val fs = FileSystem.get(s3Uri, HadoopConfiguration)
        (fs, s3Path)

      case None =>
        // Check if it's an explicit file:// URI
        if (pathString.startsWith("file://") || pathString.startsWith("file:/")) {
          val uri = new URI(pathString)
          val fs = FileSystem.get(uri, HadoopConfiguration)
          (fs, new Path(uri.getPath))
        } else {
          // Default HDFS location
          val fs = FileSystem.get(HadoopConfiguration)
          (fs, new Path(pathString))
        }
    }
  }
}
