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
package za.co.absa.spline

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.InsertIntoTest._
import za.co.absa.spline.producer.model.v1_1.WriteOperation
import za.co.absa.spline.test.fixture.spline.{SplineFixture, SplineFixture2}
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture, SparkFixture2}

class InsertIntoTest extends AsyncFlatSpec
  with Matchers
  with SparkFixture2
  with SplineFixture2 {

  "InsertInto" should "not fail when inserting to partitioned table created as Spark tables" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive (x String, ymd int) USING json PARTITIONED BY (ymd)")
            innerSpark.sql("INSERT INTO test.path_archive VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path (x String) USING json")
            innerSpark.sql("INSERT INTO test.path VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path VALUES ('Buba')")
          }


          val df = spark
            .table("test.path")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
          }
        }
      }
    }

  "ParquetTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_parquet (x String, ymd int) USING parquet PARTITIONED BY (ymd)")
            innerSpark.sql("INSERT INTO test.path_archive_parquet VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_parquet VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path_parquet (x String) USING parquet")
            innerSpark.sql("INSERT INTO test.path_parquet VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_parquet VALUES ('Buba')")
          }

          val df = spark
            .table("test.path_parquet")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_parquet")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_parquet")
            writeTable.database should be(Some("test"))
            readTable.table should be("path_parquet")
            readTable.database should be(Some("test"))
          }
        }
      }
    }

  "CsvTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>

            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_csv (x String, ymd int) USING csv PARTITIONED BY (ymd)")
            innerSpark.sql("INSERT INTO test.path_archive_csv VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_csv VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path_csv (x String) USING csv")
            innerSpark.sql("INSERT INTO test.path_csv VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_csv VALUES ('Buba')")
          }

          val df = spark
            .table("test.path_csv")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_csv")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_csv")
            writeTable.database should be(Some("test"))
            readTable.table should be("path_csv")
            readTable.database should be(Some("test"))
          }
        }
      }
    }

  "JsonTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_json (x String, ymd int) USING json PARTITIONED BY (ymd)")
            innerSpark.sql("INSERT INTO test.path_archive_json VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_json VALUES ('Tere', 20190403)")
            innerSpark.sql("CREATE TABLE if not exists test.path_json (x String) USING json")
            innerSpark.sql("INSERT INTO test.path_json VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_json VALUES ('Buba')")
          }

          val df = spark
            .table("test.path_json")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_json")
            }
          } yield {

            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_json")
            writeTable.database should be(Some("test"))
            readTable.table should be("path_json")
            readTable.database should be(Some("test"))
          }
        }
      }
    }

  "ORCTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_orc (x String, ymd int) USING orc PARTITIONED BY (ymd)")
            innerSpark.sql("INSERT INTO test.path_archive_orc VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_orc VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path_orc (x String) USING orc")
            innerSpark.sql("INSERT INTO test.path_orc VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_orc VALUES ('Buba')")
          }
          val df = spark
            .table("test.path_orc")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_orc")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_orc")
            writeTable.database should be(Some("test"))
            readTable.table should be("path_orc")
            readTable.database should be(Some("test"))
          }
        }
      }
    }

  "CsvSerdeTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_csvserde (x String, ymd int) " +
              "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ")
            innerSpark.sql("INSERT INTO test.path_archive_csvserde VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_csvserde VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path_csvserde (x String) " +
              "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ")
            innerSpark.sql("INSERT INTO test.path_csvserde VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_csvserde VALUES ('Buba')")
          }

          val df = spark
            .table("test.path_csvserde")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_csvserde")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_csvserde")
            writeTable.database should be(Some("test"))
            readTable.table should be("path_csvserde")
            readTable.database should be(Some("test"))
          }
        }
      }
    }

  "ParquetSerdeTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_parquetserde (x String, ymd int) " +
              "stored as PARQUET ")
            innerSpark.sql("INSERT INTO test.path_archive_parquetserde VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_parquetserde VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path_parquetserde (x String) " +
              "stored as PARQUET ")
            innerSpark.sql("INSERT INTO test.path_parquetserde VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_parquetserde VALUES ('Buba')")
          }

          val df = spark
            .table("test.path_parquetserde")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_parquetserde")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_parquetserde")
            writeTable.database should be(Some("test"))
            readTable.database should be(Some("test"))
            readTable.table should be("path_parquetserde")
          }
        }
      }
    }

  "OrcSerdeTable" should "Produce CatalogTable params" in
    withAsyncRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { implicit spark =>
        withLineageTracking { captor =>

          withNewSparkSession { innerSpark =>
            innerSpark.sql("CREATE DATABASE if not exists test")
            innerSpark.sql("CREATE TABLE if not exists test.path_archive_orcserde (x String, ymd int) " +
              "stored as orc ")
            innerSpark.sql("INSERT INTO test.path_archive_orcserde VALUES ('Tata', 20190401)")
            innerSpark.sql("INSERT INTO test.path_archive_orcserde VALUES ('Tere', 20190403)")

            innerSpark.sql("CREATE TABLE if not exists test.path_orcserde (x String) " +
              "stored as orc ")
            innerSpark.sql("INSERT INTO test.path_orcserde VALUES ('Monika')")
            innerSpark.sql("INSERT INTO test.path_orcserde VALUES ('Buba')")
          }

          val df = spark
            .table("test.path_orcserde")
            .withColumn("ymd", lit(20190401))

          for {
            (plan, _) <- captor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_orcserde")
            }
          } yield {
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)

            val writeTable = extractTableIdentifier(plan.operations.write.params)
            val readTable = extractTableIdentifier(plan.operations.reads.get.head.params)
            writeTable.table should be("path_archive_orcserde")
            writeTable.database should be(Some("test"))
            readTable.table should be("path_orcserde")
            readTable.database should be(Some("test"))
          }
        }
      }
    }
}

object InsertIntoTest {
  private def extractTableIdentifier(paramsOption: Option[Map[String, Any]]): TableIdentifier =
    paramsOption.get("table").asInstanceOf[Map[String, _]]("identifier").asInstanceOf[TableIdentifier]
}
