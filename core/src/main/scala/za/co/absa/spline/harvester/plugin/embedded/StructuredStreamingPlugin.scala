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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue
import za.co.absa.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.StructuredStreamingPlugin._
import za.co.absa.spline.harvester.plugin._

import javax.annotation.Priority

@Priority(Precedence.Normal)
class StructuredStreamingPlugin extends Plugin with ReadNodeProcessing with WriteNodeProcessing {

  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case `_: StreamingDataSourceV2Relation`(relational) =>
      val dataSourceReader = extractFieldValue[AnyRef](relational, "reader")
      val options = extractFieldValue[Map[String, String]](relational, "options")

      dataSourceReader match {
        case `_: KafkaMicroBatchReader`(_) =>
          val bootstrapServers = options("kafka.bootstrap.servers")
          val topic = options("subscribe")
          (SourceIdentifier(Some("kafka"), asURI(topic)), Map("bootstrap.servers" -> bootstrapServers))
      }
  }

  override val writeNodeProcessor: PartialFunction[LogicalPlan, WriteNodeInfo] = {
    case `_: WriteToDataSourceV2`(writeToDataSource) =>
      val writer = extractFieldValue[AnyRef](writeToDataSource, "writer")
      val query = extractFieldValue[LogicalPlan](writeToDataSource, "query")

      writer match {
        case `_: MicroBatchWriter`(microBatchWriter) =>
          val dataSourceWriter = extractFieldValue[AnyRef](microBatchWriter, "writer")
          dataSourceWriter match {

            case `_: KafkaStreamWriter`(kafkaStreamWriter) =>
              val topic = extractFieldValue[Option[String]](kafkaStreamWriter, "topic")
              val producerParams = extractFieldValue[Map[String, String]](kafkaStreamWriter, "producerParams")
              val bootstrapServers = producerParams("bootstrap.servers")

              (SourceIdentifier(Some("kafka"), asURI(topic.get)), SaveMode.Append, query, Map("bootstrap.servers" -> bootstrapServers))

            case `_: FileStreamWriter`(fileStreamWriter) =>
              val fileStreamSink = extractFieldValue[FileStreamSink](fileStreamWriter, "fileStreamSink")
              val path = extractFieldValue[String](fileStreamSink, "path")
              val fileFormat = extractFieldValue[DataSourceRegister](fileStreamSink, "fileFormat")
              val options = extractFieldValue[Map[String, String]](fileStreamSink, "options")

              (SourceIdentifier(Some(fileFormat.shortName()), asURI(path)), SaveMode.Append, query, options)
          }
      }
  }
}

object StructuredStreamingPlugin {
  object `_: StreamingDataSourceV2Relation` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation")

  object `_: KafkaMicroBatchReader` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.kafka010.KafkaMicroBatchReader")

  object `_: KafkaStreamWriter` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.kafka010.KafkaStreamWriter")

  object `_: WriteToDataSourceV2` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2")

  object `_: MicroBatchWriter` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter")

  object `_: FileStreamWriter` extends SafeTypeMatchingExtractor[AnyRef](
    "za.co.absa.spline.harvester.plugin.embedded.FileStreamWriter")

  private def asURI(topic: String) = s"kafka:$topic"
}

// https://issues.apache.org/jira/browse/SPARK-27484
class FileStreamWriter(val fileStreamSink: FileStreamSink) extends StreamWriter {
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriterFactory(): DataWriterFactory[InternalRow] = null
}
