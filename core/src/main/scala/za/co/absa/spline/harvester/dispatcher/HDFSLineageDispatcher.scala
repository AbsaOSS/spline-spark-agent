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
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import scala.concurrent.blocking

/**
 * A port of https://github.com/AbsaOSS/spline/tree/release/0.3.9/persistence/hdfs/src/main/scala/za/co/absa/spline/persistence/hdfs
 *
 * Note:
 * This class is unstable, experimental, is mostly used for debugging, with no guarantee to work properly
 * for every generic use case in a real production application.
 *
 * It is NOT thread-safe, strictly synchronous assuming a predefined order of method calls: `send(plan)` and then `send(event)`
 *
 * TWO MODES OF OPERATION:
 *
 * 1. DEFAULT MODE (customLineagePath = None, null, or empty string):
 *    Lineage files are written alongside the target data files.
 *    Example: Writing to /data/output/file.parquet creates /data/output/_LINEAGE
 *
 * 2. CENTRALIZED MODE (customLineagePath set to a valid path):
 *    All lineage files are written to a single centralized location with unique filenames.
 *    Filename format: {timestamp}_{fileName}_{appId}
 *    - timestamp: Human-readable UTC timestamp (yyyy-MM-dd_HH-mm-ss-SSS) for chronological sorting and filtering
 *    - fileName: The configured fileName value (e.g., "my_file.parq_LINEAGE")
 *    - appId: Spark application ID for traceability
 *
 *    The timestamp-first format ensures natural chronological sorting and easy date-based filtering.
 *    Parent directories are automatically created with proper permissions for multi-user access (HDFS/local).
 *    For object storage (S3, GCS, Azure), directory creation is skipped since they use key prefixes.
 */
@Experimental
class HDFSLineageDispatcher(filename: String, permission: FsPermission, bufferSize: Int, customLineagePath: Option[String])
  extends LineageDispatcher
    with Logging {

  def this(conf: Configuration) = this(
    filename = conf.getRequiredString(FileNameKey),
    permission = new FsPermission(conf.getRequiredObject(FilePermissionsKey).toString),
    bufferSize = conf.getRequiredInt(BufferSizeKey),
    customLineagePath = conf.getOptionalString(CustomLineagePathKey)
  )

  @volatile
  private var _lastSeenPlan: ExecutionPlan = _

  override def name = "HDFS"

  override def send(plan: ExecutionPlan): Unit = {
    this._lastSeenPlan = plan
  }

  override def send(event: ExecutionEvent): Unit = {
    // check state
    if (this._lastSeenPlan == null || this._lastSeenPlan.id.get != event.planId)
      throw new IllegalStateException("send(event) must be called strictly after send(plan) method with matching plan ID")

    try {
      val path = resolveLineagePath(event.planId.toString)
      val planWithEvent = Map(
        "executionPlan" -> this._lastSeenPlan,
        "executionEvent" -> event
      )

      import HarvesterJsonSerDe.impl._
      persistToHadoopFs(planWithEvent.toJson, path)
    } finally {
      this._lastSeenPlan = null
    }
  }

  /**
   * Resolves the lineage file path based on configuration.
   * If customLineagePath is specified, lineage files are written to that centralized location.
   * Otherwise, lineage files are written alongside the target data file (current behavior).
   *
   * @param planId The execution plan ID used for generating unique filenames in centralized mode
   * @return The full path where the lineage file should be written
   */
  private def resolveLineagePath(planId: String): String = {
    customLineagePath match {
      case Some(customPath) =>
        // Centralized mode: write to custom path with unique filename
        val cleanCustomPath = customPath.stripSuffix("/")
        val uniqueFilename = generateUniqueFilename(planId)
        s"$cleanCustomPath/$uniqueFilename"
      case None =>
        // Default mode: write alongside target data file
        s"${this._lastSeenPlan.operations.write.outputSource.stripSuffix("/")}/$filename"
    }
  }

  /**
   * Generates a unique filename for centralized lineage storage.
   * 
   * Format: {timestamp}_{fileName}_{appId}
   * Example: 2025-10-12_14-30-45-123_lineage_app-20251012143045-0001
   *
   * This format optimizes for operational debugging use cases:
   * - Timestamp FIRST: Ensures natural chronological sorting (most recent files appear together)
   * - Application ID: Full traceability to specific Spark application run
   *
   * @param planId The execution plan ID (unused, kept for interface compatibility)
   * @return A unique filename optimized for filtering and sorting
   */
  private def generateUniqueFilename(planId: String): String = {
    val sparkContext = SparkContext.getOrCreate()
    val appId = sparkContext.applicationId
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS").withZone(ZoneId.of("UTC"))
    val timestamp = dateFormatter.format(Instant.now())
    s"${timestamp}_${filename}_${appId}"
  }

  /**
   * Ensures that parent directories exist with proper permissions for multi-user access.
   * This is critical for centralized lineage storage where different service accounts need write access.
   *
   * Note: Object storage systems (S3, GCS, Azure Blob) don't have true directories - they use key prefixes.
   * Directory creation is automatically skipped for these systems to avoid unnecessary operations.
   *
   * @param fs The Hadoop FileSystem
   * @param path The target file path whose parent directories should be created
   */
  private def ensureParentDirectoriesExist(fs: FileSystem, path: Path): Unit = {
    // Object storage systems (S3, GCS, Azure Blob) don't have real directories - they're just key prefixes
    // Skip directory creation for these systems to avoid unnecessary operations
    val fsScheme = fs.getUri.getScheme
    val isObjectStorage = fsScheme != null && (
      fsScheme.startsWith("s3") ||      // S3: s3, s3a, s3n
      fsScheme.startsWith("gs") ||      // Google Cloud Storage: gs
      fsScheme.startsWith("wasb") ||    // Azure Blob Storage: wasb, wasbs
      fsScheme.startsWith("abfs") ||    // Azure Data Lake Storage Gen2: abfs, abfss
      fsScheme.startsWith("adl")        // Azure Data Lake Storage Gen1: adl
    )
    
    if (isObjectStorage) {
      logDebug(s"Skipping directory creation for object storage filesystem ($fsScheme) - directories are implicit key prefixes")
      return
    }

    val parentDir = path.getParent
    if (parentDir != null && !fs.exists(parentDir)) {
      try {
        // Create directories with multi-user friendly permissions to allow all service accounts to write
        // This uses the same permission object that's already configured for files
        val created = fs.mkdirs(parentDir, permission)
        if (created) {
          logInfo(s"Created parent directories: $parentDir with permissions $permission")
        }
      } catch {
        case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
          // Race condition: another process created the directory - this is fine
          logDebug(s"Directory $parentDir already exists (created by another process)")
        case e: Exception =>
          logWarning(s"Failed to create parent directories: $parentDir", e)
          throw e
      }
    }
  }

  private def persistToHadoopFs(content: String, fullLineagePath: String): Unit = blocking {
    val (fs, path) = pathStringToFsWithPath(fullLineagePath)
    logDebug(s"Opening HadoopFs output stream to $path")

    // Ensure parent directories exist with proper permissions for multi-user access
    ensureParentDirectoriesExist(fs, path)

    val replication = fs.getDefaultReplication(path)
    val blockSize = fs.getDefaultBlockSize(path)
    val outputStream = fs.create(path, permission, true, bufferSize, replication, blockSize, null)

    val umask = FsPermission.getUMask(fs.getConf)
    FsPermission.getFileDefault.applyUMask(umask)

    logDebug(s"Writing lineage to $path")
    using(outputStream) {
      _.write(content.getBytes("UTF-8"))
    }
  }
}

object HDFSLineageDispatcher {
  private val HadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration

  private val FileNameKey = "fileName"
  private val FilePermissionsKey = "filePermissions"
  private val BufferSizeKey = "fileBufferSize"
  private val CustomLineagePathKey = "customLineagePath"

  /**
   * Converts string full path to Hadoop FS and Path, e.g.
   * `s3://mybucket1/path/to/file` -> S3 FS + `path/to/file`
   * `/path/on/hdfs/to/file` -> local HDFS + `/path/on/hdfs/to/file`
   *
   * Note, that non-local HDFS paths are not supported in this method, e.g. hdfs://nameservice123:8020/path/on/hdfs/too.
   *
   * @param pathString path to convert to FS and relative path
   * @return FS + relative path
   */
  def pathStringToFsWithPath(pathString: String): (FileSystem, Path) = {
    pathString.toSimpleS3Location match {
      case Some(s3Location) =>
        val s3Uri = new URI(s3Location.asSimpleS3LocationString) // s3://<bucket>
        val s3Path = new Path(s"/${s3Location.path}") // /<text-file-object-path>

        val fs = FileSystem.get(s3Uri, HadoopConfiguration)
        (fs, s3Path)

      case None => // local hdfs location
        val fs = FileSystem.get(HadoopConfiguration)
        (fs, new Path(pathString))
    }
  }
}
