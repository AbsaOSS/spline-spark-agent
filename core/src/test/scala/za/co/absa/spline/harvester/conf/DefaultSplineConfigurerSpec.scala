/*
 * Copyright 2017 ABSA Group Limited
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

import org.apache.commons.configuration.{Configuration, MapConfiguration}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import za.co.absa.spline.harvester.dispatcher.{AggregateLineageDispatcher, LineageDispatcher}
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import scala.collection.JavaConverters._

class DefaultSplineConfigurerTest extends AnyFlatSpec {

  it should "return a simple lineage dispatcher if only the primary dispatcher is defined" in {
    val config = new MapConfiguration(Map(
      "spline.lineage_dispatcher.className" -> "za.co.absa.spline.harvester.conf.LineageDispatcherA"
    ).asJava)

    new DefaultSplineConfigurer(config).lineageDispatcher shouldBe a [LineageDispatcherA]
  }

  it should "return an aggregate lineage dispatcher if secondary dispatchers are defined" in {
    val config = new MapConfiguration(Map(
      "spline.lineage_dispatcher.className" -> "za.co.absa.spline.harvester.conf.LineageDispatcherA",
      "spline.lineage_dispatcher.secondary.classNames" ->
      "za.co.absa.spline.harvester.conf.LineageDispatcherB, za.co.absa.spline.harvester.conf.LineageDispatcherC"

    ).asJava)

    val configurer = new DefaultSplineConfigurer(config)
    configurer.lineageDispatcher shouldBe an [AggregateLineageDispatcher]
    val aggregateDispatcher: AggregateLineageDispatcher = configurer.lineageDispatcher.asInstanceOf[AggregateLineageDispatcher]

    aggregateDispatcher.primaryDelegate shouldBe a [LineageDispatcherA]
    aggregateDispatcher.maybeSecondaryDelegates.isDefined shouldBe true
    val secondaryDispatchers: Seq[LineageDispatcher] = aggregateDispatcher.maybeSecondaryDelegates.get
    secondaryDispatchers.length shouldBe 2
    secondaryDispatchers(0) shouldBe a [LineageDispatcherB]
    secondaryDispatchers(1) shouldBe a [LineageDispatcherC]
  }
}

class LineageDispatcherA(config: Configuration) extends LineageDispatcher {
  override def send(executionPlan: ExecutionPlan): String = "A"

  override def send(event: ExecutionEvent): Unit = {}
}

class LineageDispatcherB(config: Configuration) extends LineageDispatcher {

  override def send(executionPlan: ExecutionPlan): String = "B"

  override def send(event: ExecutionEvent): Unit = {}
}

class LineageDispatcherC(config: Configuration) extends LineageDispatcher {

  override def send(executionPlan: ExecutionPlan): String = "C"

  override def send(event: ExecutionEvent): Unit = {}
}
