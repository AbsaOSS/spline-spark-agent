package za.co.absa.spline.harvester.dispatcher.sqsdispatcher

import org.apache.commons.configuration.Configuration
import za.co.absa.commons.config.ConfigurationImplicits._
import za.co.absa.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.sqsdispatcher.SqsLineageDispatcherConfig._

import java.time.Duration
import java.time.temporal.ChronoUnit

object SqsLineageDispatcherConfig {
  val QueueUrl = "queue.url"
  val ApiVersion = "apiVersion"

  def apply(c: Configuration) = new SqsLineageDispatcherConfig(c)
}

class SqsLineageDispatcherConfig(config: Configuration) {
  val queueUrl: String = config.getRequiredString(QueueUrl)
  val apiVersion: Version = Version.asSimple(config.getString(ApiVersion, "1.2"))
}
