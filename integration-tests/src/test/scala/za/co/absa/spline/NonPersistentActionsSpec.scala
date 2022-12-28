/*
 * Copyright 2019 ABSA Group Limited
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


package za.co.absa.spline

import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.NonPersistentActionsSpec.NonPersistentActionsPluginEnabledProperty
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture

class NonPersistentActionsSpec extends AsyncFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture {

  for (
    (action, methodName, expectedCommandName) <- Array[(Dataset[_] => Unit, String, String)](
      (_.count(), "count", "count"),
      (_.collect(), "collect", "collect"),
      (_.collectAsList(), "collectAsList", "collectAsList"),
      (_.show(), "show", "head"),
      (_.toLocalIterator(), "toLocalIterator", "toLocalIterator")
    )
  ) yield {
    it should s"capture lineage from the `$methodName()` action" in
      withIsolatedSparkSession(_.config(NonPersistentActionsPluginEnabledProperty, value = true)) { implicit spark =>
        withLineageTracking { captor =>
          for {
            (plan, _) <- captor.lineageOf {
              import spark.implicits._
              val df1 = Seq((1, 2, 3)).toDF()
              action(df1)
            }
          } yield {
            plan.operations.write.append shouldBe false
            plan.operations.write.name shouldEqual expectedCommandName
            plan.operations.write.extra should contain("synthetic" -> true)
            plan.operations.write.outputSource should startWith("memory://")
            plan.operations.write.outputSource should include("jvm_")
          }
        }
      }
  }
}

object NonPersistentActionsSpec {
  private val NonPersistentActionsPluginEnabledProperty = "spline.plugins.za.co.absa.spline.harvester.plugin.embedded.NonPersistentActionsCapturePlugin.enabled"
}

