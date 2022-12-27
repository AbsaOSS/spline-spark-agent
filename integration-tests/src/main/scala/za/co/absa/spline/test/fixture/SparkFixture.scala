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
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.SparkFixture._

import java.sql.DriverManager
import scala.concurrent.Future
import scala.util.Try

trait SparkFixture {
  this: AsyncTestSuite =>

  def withSparkSession[T](testBody: SparkSession => T): T = {
    testBody(sessionBuilder().getOrCreate)
  }

  def withNewSparkSession[T](testBody: SparkSession => T): T = {
    testBody(sessionBuilder().getOrCreate.newSession)
  }

  def withRestartingSparkSession[T](builderCustomizer: SparkSession.Builder => SparkSession.Builder = identity)(testBody: SparkSession => T): T = {
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
