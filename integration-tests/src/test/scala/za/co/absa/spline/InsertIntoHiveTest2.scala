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

import org.apache.spark.sql.SparkSession
import org.scalatest.Succeeded
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.SparkFixture2
import za.co.absa.spline.test.fixture.spline.SplineFixture2

//class InsertIntoHiveTest2 extends AnyFlatSpec {
//
//  "InsertInto" should "produce lineage when inserting to partitioned table created as Hive table" in {
//
//    val warehouseDir: String = TempDirectory("SparkFixture", "UnitTest", pathOnly = true).deleteOnExit().path.toString.stripSuffix("/")
//
//    val spark =  SparkSession.builder
//      .master("local")
//      .config("spark.sql.warehouse.dir", warehouseDir)
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .enableHiveSupport()
//      .getOrCreate().newSession()
//
//
//    spark.sql("DROP TABLE if exists path_archive ")
//
//    // PARTITIONED BY is causing SIGSEGV
//    // spark.sql("CREATE TABLE if not exists path_archive (x String, ymd int) USING hive PARTITIONED BY (ymd)")
//    spark.sql("CREATE TABLE if not exists path_archive (x String, ymd int) USING hive")
//
//    spark.sql("INSERT INTO path_archive VALUES ('Tata', 20190401)") // <- SIGSEGV
//  }
//}
