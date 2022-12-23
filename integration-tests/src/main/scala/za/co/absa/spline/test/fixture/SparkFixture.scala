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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{AsyncTestSuite, BeforeAndAfterEach}
import za.co.absa.commons.io.TempDirectory

import java.io.File
import java.net.URI
import java.sql.DriverManager
import scala.concurrent.Future
import scala.util.Try

trait SparkFixture {
  this: AsyncTestSuite =>

  val baseDir: TempDirectory = TempDirectory("SparkFixture", "UnitTest", pathOnly = true).deleteOnExit()
  val warehouseDir: String = baseDir.asString.stripSuffix("/")
  val warehouseUri: URI = baseDir.toURI
  val metastoreDir: String = TempDirectory("Metastore", "debug", pathOnly = true).deleteOnExit().asString
  val metastoreConnectURL = s"jdbc:derby:$metastoreDir/metastore_db;create=true"

  protected def sessionBuilder: SparkSession.Builder = {
    SparkSession.builder
      .master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.ui.enabled", "false")
      .config("javax.jdo.option.ConnectionURL", metastoreConnectURL)
  }

  def withSparkSession[T](testBody: SparkSession => T): T = {
    testBody(sessionBuilder.getOrCreate)
  }

  def withNewSparkSession[T](testBody: SparkSession => T): T = {
    testBody(sessionBuilder.getOrCreate.newSession)
  }

  def withRestartingSparkSession[T](builderCustomizer: SparkSession.Builder => SparkSession.Builder = identity)(testBody: SparkSession => T): T = {
    haltSparkAndCleanup()
    val res = testBody(builderCustomizer(sessionBuilder).getOrCreate)
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

  private[SparkFixture] def haltSparkAndCleanup(): Unit = {
    SparkSession.getDefaultSession.foreach(_.close())
    // clean up Derby resources to allow for re-creation of a Hive context later in the same JVM instance
    Try(DriverManager.getConnection("jdbc:derby:;shutdown=true"))
    FileUtils.deleteQuietly(new File("metastore_db"))
    FileUtils.deleteQuietly(new File(warehouseDir))
  }
}

object SparkFixture {

  trait NewPerTest extends SparkFixture with BeforeAndAfterEach {
    this: AsyncTestSuite =>

    override protected def beforeEach(): Unit = {
      haltSparkAndCleanup()
      super.beforeEach()
    }

    override protected def afterEach(): Unit = {
      try super.afterEach()
      finally haltSparkAndCleanup()
    }
  }

}
