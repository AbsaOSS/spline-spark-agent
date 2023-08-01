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

package za.co.absa.spline.issue

import za.co.absa.spline.SparkApp
import za.co.absa.spline.commons.io.TempDirectory

/**
 * This Job requires Spark 3 or higher
 */
object DeltaMergeDSV2Job extends SparkApp(
  name = "DeltaMergeDSV2Job",
  conf = Seq(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
) {
  val path = TempDirectory().deleteOnExit().path

  import za.co.absa.spline.harvester.SparkLineageInitializer._

  // Initializing library to hook up to Apache Spark
  spark.enableLineageTracking()

  spark.sql(s"CREATE DATABASE dsv2 LOCATION '$path'")

  spark.sql("CREATE TABLE dsv2.foo (id INT, code STRING, name STRING) USING DELTA")
  spark.sql("INSERT INTO dsv2.foo VALUES (1014, 'PLN', 'Warsaw'), (1002, 'FRA', 'Corte')")

  spark.sql("CREATE TABLE dsv2.fooUpdate (id INT, name STRING) USING DELTA")
  spark.sql("INSERT INTO dsv2.fooUpdate VALUES (1014, 'Lodz'), (1003, 'Prague')")

  spark.sql("CREATE TABLE dsv2.barUpdate (id INT, name STRING) USING DELTA")
  spark.sql("INSERT INTO dsv2.barUpdate VALUES (4242, 'Paris'), (3342, 'Bordeaux')")

  spark.sql("UPDATE dsv2.foo SET name = 'Korok' WHERE id == 1002")

  spark.sql(
    """
      | CREATE OR REPLACE VIEW tempview AS
      |   SELECT * FROM dsv2.fooUpdate
      |   UNION
      |   SELECT * FROM dsv2.barUpdate
      |""".stripMargin
  )

  spark.sql(
    """
      | MERGE INTO dsv2.foo AS dst
      | USING tempview AS src
      | ON dst.id = src.id
      | WHEN MATCHED THEN
      |   UPDATE SET
      |     NAME = src.name
      | WHEN NOT MATCHED
      |  THEN INSERT (id, name)
      |  VALUES (src.id, src.name)
      |""".stripMargin
  ).show

  spark.read.table("dsv2.foo").show()

  spark.sql("DELETE FROM dsv2.foo WHERE ID == 1014")
}
