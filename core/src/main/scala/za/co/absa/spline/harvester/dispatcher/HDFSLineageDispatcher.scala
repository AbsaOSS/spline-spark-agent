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
import org.apache.spark.annotation.InterfaceStability.Unstable
import org.apache.spark.internal.Logging
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.commons.json.DefaultJacksonJsonSerDe
import za.co.absa.commons.lang.ARM._
import za.co.absa.spline.harvester.dispatcher.HDFSLineageDispatcher._
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

import scala.concurrent.blocking

/**
 * A port of https://github.com/AbsaOSS/spline/tree/release/0.3.9/persistence/hdfs/src/main/scala/za/co/absa/spline/persistence/hdfs
 *
 * Note:
 * This class is unstable, experimental, is mostly used for debugging, with no guarantee to work properly
 * for every generic use case in a real production application.
 *
 * It is NOT thread-safe, strictly synchronous assuming a predefined order of method calls: `send(plan)` and then `send(event)`
 */
@Unstable
class HDFSLineageDispatcher(filename: String, permission: FsPermission, bufferSize: Int)
  extends LineageDispatcher
    with DefaultJacksonJsonSerDe
    with Logging {

  def this(conf: Configuration) = this(
    filename = conf.getRequiredString(FileNameKey),
    permission = new FsPermission(conf.getOptionalString(FilePermissionsKey).getOrElse(DefaultFilePermission.toShort.toString)),
    bufferSize = conf.getRequiredInt(BufferSizeKey)
  )

  @volatile
  private var _lastSeenPlan: ExecutionPlan = _

  override def send(plan: ExecutionPlan): Unit = {
    this._lastSeenPlan = plan
  }

  override def send(event: ExecutionEvent): Unit = {
    // check state
    if (this._lastSeenPlan == null || this._lastSeenPlan.id.get != event.planId)
      throw new IllegalStateException("send(event) must be called strictly after send(plan) method with matching plan ID")

    try {
      val path = new Path(this._lastSeenPlan.operations.write.outputSource, filename)
      val planWithEvent = Map(
        "executionPlan" -> this._lastSeenPlan,
        "executionEvent" -> event
      )
      persistToHdfs(planWithEvent.toJson, path)
    } finally {
      this._lastSeenPlan = null
    }
  }

  private def persistToHdfs(content: String, path: Path): Unit = blocking {
    logDebug(s"Opening HDFS output stream to $path")
    val replication = HadoopFileSystem.getDefaultReplication(path)
    val blockSize = HadoopFileSystem.getDefaultBlockSize(path)
    val outputStream = HadoopFileSystem.create(path, permission, true, bufferSize, replication, blockSize, null)

    logDebug(s"Writing lineage to $path")
    using(outputStream) {
      _.write(content.getBytes("UTF-8"))
    }
  }
}

object HDFSLineageDispatcher {
  private val HadoopConfiguration = SparkContext.getOrCreate().hadoopConfiguration
  private val HadoopFileSystem = FileSystem.get(HadoopConfiguration)

  private val FileNameKey = "fileName"
  private val FilePermissionsKey = "filePermissions"
  private val BufferSizeKey = "fileBufferSize"

  private val DefaultFilePermission = {
    val umask = FsPermission.getUMask(HadoopFileSystem.getConf)
    FsPermission.getFileDefault.applyUMask(umask)
  }
}
