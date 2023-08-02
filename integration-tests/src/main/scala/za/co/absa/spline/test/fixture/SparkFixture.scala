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

package za.co.absa.spline.test.fixture

import org.apache.spark.sql.SparkSession
import org.scalatest.AsyncTestSuite
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.SparkFixture._

import java.sql.DriverManager
import scala.concurrent.Future
import scala.util.Try

trait SparkFixture {
  this: AsyncTestSuite =>

  /**
   * Returns a new session with isolated SQL configurations, temporary tables, registered functions are isolated,
   * but sharing the underlying `SparkContext` and cached data.
   *
   * NOTE:
   * as the returning session shares the underlying `SparkContext` the method completes quickly, but it might
   * create undesired dependency between tests.
   * To properly isolate Spark context use [[withIsolatedSparkSession]] method instead.
   */
  def withNewSparkSession[T](testBody: SparkSession => T): T = {
    val activeSession = sessionBuilder().getOrCreate
    // always call `newSession` to minimize dependencies between tests using this fixture
    val newSession = activeSession.newSession
    testBody(newSession)
  }

  /**
   *
   * Create a completely new, isolated Spark session. The `SparkContext` is restarted before and after the test.
   *
   * This method provides high level of test isolation, but because `SparkContext` is restarted, it takes significantly
   * longer to complete in comparison with the [[withNewSparkSession]] method.
   */
  def withIsolatedSparkSession[T](builderCustomizer: SparkSession.Builder => SparkSession.Builder = identity)(testBody: SparkSession => T): T = {
    haltSparkAndCleanup()
    val sparkSession = builderCustomizer(sessionBuilder()).getOrCreate
    val res = testBody(sparkSession)
    res match {
      case ft: Future[_] =>
        ft.andThen {
          case _ => haltSparkAndCleanup()
        }.asInstanceOf[T]
      case _ =>
        haltSparkAndCleanup()
        res
    }
  }
}

object SparkFixture {
  private def sessionBuilder(): SparkSession.Builder = {
    val warehouseDir: String = TempDirectory("SparkFixture_", ".warehouse", pathOnly = true).deleteOnExit().asString
    val metastoreDir: String = TempDirectory("SparkFixture_", ".metastore", pathOnly = true).deleteOnExit().asString
    SparkSession.builder
      .master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:$metastoreDir/metastore_db;create=true")
  }

  private def haltSparkAndCleanup(): Unit = {
    SparkSession.getDefaultSession.foreach(_.close())
    // clean up Derby resources to allow for re-creation of a Hive context later in the same JVM instance
    Try(DriverManager.getConnection("jdbc:derby:;shutdown=true"))
  }
}
