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

package za.co.absa.spline.example.batch

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import za.co.absa.spline.SparkApp

object ViewAttJob extends SparkApp(
  name = "ViewAttJob",
  conf = Seq( CATALOG_IMPLEMENTATION.key -> "hive")
) {
  import org.apache.spark.sql._
  import za.co.absa.spline.harvester.SparkLineageInitializer._

  // Initializing library to hook up to Apache Spark
  spark.enableLineageTracking()

  // A business logic of a spark job ...

  val sourceDS = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/input/batch/wikidata.csv")

  sourceDS
    .write
    .format("orc")
    .mode("overwrite")
    .saveAsTable("test_source")

  spark.sql("""DROP VIEW IF EXISTS test_source_vw""")
  spark.sql("""CREATE VIEW test_source_vw AS SELECT * FROM test_source""")

  spark.sql("select * from test_source_vw")
    //.select($"productName", ($"quantitySold" * $"pricePerUnit").as("totalEarned"))
    .write
    .format("orc")
    .mode("overwrite")
    .saveAsTable("view_test_target")
}
