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

package za.co.absa.spline.harvester.conf

import org.apache.commons.configuration.Configuration
import za.co.absa.commons.HierarchicalObjectFactory.ClassName
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer.ConfProperty._
import za.co.absa.spline.harvester.conf.Spline05ConfigurationAdapter._
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpLineageDispatcherConfig._
import za.co.absa.spline.harvester.iwd.DefaultIgnoredWriteDetectionStrategy._

import java.util
import scala.collection.JavaConverters.asJavaIterableConverter

class Spline05ConfigurationAdapter(configuration: Configuration) extends ReadOnlyConfiguration {

  private val defaultValues =
    if (configuration.containsKey(DeprecatedDispatcherClassName))
      Map(RootLineageDispatcher -> DefaultDispatcherNameValue)
    else
      Map.empty[String, AnyRef]

  private def scalaKeys = keyMap.keys.filter(containsKey) ++ defaultValues.keys

  override def isEmpty: Boolean = scalaKeys.isEmpty

  override def containsKey(key: String): Boolean = keyMap
    .get(key)
    .map(spline05key => configuration.containsKey(spline05key))
    .getOrElse(defaultValues.contains(key))

  override def getProperty(key: String): AnyRef = keyMap
    .get(key)
    .map(spline05key => configuration.getProperty(spline05key))
    .orElse(defaultValues.get(key))
    .orNull

  override def getKeys: util.Iterator[String] = scalaKeys.asJava.iterator()

}

object Spline05ConfigurationAdapter {
  private val DefaultDispatcherNameValue = "http"

  private val DefaultDispatcherPrefix =
    s"$RootLineageDispatcher.$DefaultDispatcherNameValue"

  private val DeprecatedDispatcherClassName = "spline.lineage_dispatcher.className"

  private val keyMap = Map(
    IgnoreWriteDetectionStrategyClass -> "spline.iwd_strategy.className",
    OnMissingMetricsKey -> "spline.iwd_strategy.default.on_missing_metrics",
    UserExtraMetadataProviderClass -> "spline.user_extra_meta_provider.className",

    s"$DefaultDispatcherPrefix.$ClassName" -> DeprecatedDispatcherClassName,
    s"$DefaultDispatcherPrefix.$ProducerUrlProperty" -> "spline.producer.url",
    s"$DefaultDispatcherPrefix.$ConnectionTimeoutMsKey" -> "spline.timeout.connection",
    s"$DefaultDispatcherPrefix.$ReadTimeoutMsKey" -> "spline.timeout.read"
  )

}
