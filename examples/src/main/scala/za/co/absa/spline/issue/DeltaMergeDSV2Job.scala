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

import za.co.absa.commons.io.TempDirectory
import za.co.absa.spline.SparkApp

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

  spark.sql("CREATE TABLE dsv2.foo ( ID int, NAME string ) USING DELTA")
  spark.sql("INSERT INTO dsv2.foo VALUES (1014, 'Warsaw'), (1002, 'Corte')")

  spark.sql("CREATE TABLE dsv2.fooUpdate ( ID Int, NAME String ) USING DELTA")
  spark.sql("INSERT INTO dsv2.fooUpdate VALUES (1014, 'Lodz'), (1003, 'Prague')")

  spark.sql("CREATE TABLE dsv2.barUpdate ( ID Int, NAME String ) USING DELTA")
  spark.sql("INSERT INTO dsv2.barUpdate VALUES (4242, 'Paris'), (3342, 'Bordeaux')")

  spark.sql(s"UPDATE dsv2.foo SET NAME = 'Korok' WHERE ID == 1002")

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
      | MERGE INTO dsv2.foo
      | USING tempview AS foobar
      | ON dsv2.foo.ID = foobar.ID
      | WHEN MATCHED THEN
      |   UPDATE SET
      |     NAME = foobar.NAME
      | WHEN NOT MATCHED
      |  THEN INSERT (ID, NAME)
      |  VALUES (foobar.ID, foobar.NAME)
      |""".stripMargin
  )

  spark.sql(s"DELETE FROM dsv2.foo WHERE ID == 1014")
}
