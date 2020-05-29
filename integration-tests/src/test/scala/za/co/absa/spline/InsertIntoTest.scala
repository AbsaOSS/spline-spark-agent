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
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{SparkDatabaseFixture, SparkFixture}
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._
class InsertIntoTest extends AnyFlatSpec with Matchers with SparkFixture with SparkDatabaseFixture with SplineFixture {

  "InsertInto" should "not fail when inserting to partitioned table created as Spark tables" in
    withRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { spark =>
        withLineageTracking(spark)(lineageCaptor => {

          spark.sql("CREATE DATABASE if not exists test")
          spark.sql("CREATE TABLE if not exists test.path_archive (x String, ymd int) USING json PARTITIONED BY (ymd)")
          spark.sql("INSERT INTO test.path_archive VALUES ('Tata', 20190401)")
          spark.sql("INSERT INTO test.path_archive VALUES ('Tere', 20190403)")

          spark.sql("CREATE TABLE if not exists test.path (x String) USING json")
          spark.sql("INSERT INTO test.path VALUES ('Monika')")
          spark.sql("INSERT INTO test.path VALUES ('Buba')")

          val df = spark
            .table("test.path")
            .withColumn("ymd", lit(20190401))

          val (plan, _) = lineageCaptor.lineageOf {
            df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive")
          }
          plan.operations.write.outputSource should include("path_archive")
          plan.operations.write.append should be(false)

        })
      }
    }

  "ParquetTable" should "Produce CatalogTable params" in
    withRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { spark =>
        withLineageTracking(spark)(lineageCaptor => {

          spark.sql("CREATE DATABASE if not exists test")
          spark.sql("CREATE TABLE if not exists test.path_archive_parquet (x String, ymd int) USING parquet PARTITIONED BY (ymd)")
          spark.sql("INSERT INTO test.path_archive_parquet VALUES ('Tata', 20190401)")
          spark.sql("INSERT INTO test.path_archive_parquet VALUES ('Tere', 20190403)")

          spark.sql("CREATE TABLE if not exists test.path_parquet (x String) USING parquet")
          spark.sql("INSERT INTO test.path_parquet VALUES ('Monika')")
          spark.sql("INSERT INTO test.path_parquet VALUES ('Buba')")

          val df = spark
            .table("test.path_parquet")
            .withColumn("ymd", lit(20190401))

          val (plan, _) = lineageCaptor.lineageOf {
            df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_parquet")
          }
          plan.operations.write.outputSource should include("path_archive")
          plan.operations.write.append should be(false)
          plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_parquet")
          plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
          plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_parquet")
          plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

        })
      }
    }

  "CsvTable" should "Produce CatalogTable params" in
    withRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { spark =>
        withLineageTracking(spark)(lineageCaptor => {

          spark.sql("CREATE DATABASE if not exists test")
          spark.sql("CREATE TABLE if not exists test.path_archive_csv (x String, ymd int) USING csv PARTITIONED BY (ymd)")
          spark.sql("INSERT INTO test.path_archive_csv VALUES ('Tata', 20190401)")
          spark.sql("INSERT INTO test.path_archive_csv VALUES ('Tere', 20190403)")

          spark.sql("CREATE TABLE if not exists test.path_csv (x String) USING csv")
          spark.sql("INSERT INTO test.path_csv VALUES ('Monika')")
          spark.sql("INSERT INTO test.path_csv VALUES ('Buba')")

          val df = spark
            .table("test.path_csv")
            .withColumn("ymd", lit(20190401))

          val (plan, _) = lineageCaptor.lineageOf {
            df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_csv")
          }
          plan.operations.write.outputSource should include("path_archive")
          plan.operations.write.append should be(false)
          plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_csv")
          plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
          plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_csv")
          plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

        })
      }
    }

  "JsonTable" should "Produce CatalogTable params" in
    withRestartingSparkContext {
      withCustomSparkSession(_.enableHiveSupport()) { spark =>
        withLineageTracking(spark)(lineageCaptor => {

          spark.sql("CREATE DATABASE if not exists test")
          spark.sql("CREATE TABLE if not exists test.path_archive_json (x String, ymd int) USING json PARTITIONED BY (ymd)")
          spark.sql("INSERT INTO test.path_archive_json VALUES ('Tata', 20190401)")
          spark.sql("INSERT INTO test.path_archive_json VALUES ('Tere', 20190403)")

          spark.sql("CREATE TABLE if not exists test.path_json (x String) USING json")
          spark.sql("INSERT INTO test.path_json VALUES ('Monika')")
          spark.sql("INSERT INTO test.path_json VALUES ('Buba')")

          val df = spark
            .table("test.path_json")
            .withColumn("ymd", lit(20190401))

          val (plan, _) = lineageCaptor.lineageOf {
            df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_json")
          }
          plan.operations.write.outputSource should include("path_archive")
          plan.operations.write.append should be(false)
          plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_json")
          plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
          plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_json")
          plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

        })
      }
    }


    "ORCTable" should "Produce CatalogTable params" in
      withRestartingSparkContext {
        withCustomSparkSession(_.enableHiveSupport()) { spark =>
          withLineageTracking(spark)(lineageCaptor => {

            spark.sql("CREATE DATABASE if not exists test")
            spark.sql("CREATE TABLE if not exists test.path_archive_orc (x String, ymd int) USING orc PARTITIONED BY (ymd)")
            spark.sql("INSERT INTO test.path_archive_orc VALUES ('Tata', 20190401)")
            spark.sql("INSERT INTO test.path_archive_orc VALUES ('Tere', 20190403)")

            spark.sql("CREATE TABLE if not exists test.path_orc (x String) USING orc")
            spark.sql("INSERT INTO test.path_orc VALUES ('Monika')")
            spark.sql("INSERT INTO test.path_orc VALUES ('Buba')")

            val df = spark
              .table("test.path_orc")
              .withColumn("ymd", lit(20190401))

            val (plan, _) = lineageCaptor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_orc")
            }
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_orc")
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_orc")
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

          })
        }
      }

    "CsvSerdeTable" should "Produce CatalogTable params" in
      withRestartingSparkContext {
        withCustomSparkSession(_.enableHiveSupport()) { spark =>
          withLineageTracking(spark)(lineageCaptor => {

            spark.sql("CREATE DATABASE if not exists test")
            spark.sql("CREATE TABLE if not exists test.path_archive_csvserde (x String, ymd int) " +
              "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ")
            spark.sql("INSERT INTO test.path_archive_csvserde VALUES ('Tata', 20190401)")
            spark.sql("INSERT INTO test.path_archive_csvserde VALUES ('Tere', 20190403)")

            spark.sql("CREATE TABLE if not exists test.path_csvserde (x String) " +
              "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ")
            spark.sql("INSERT INTO test.path_csvserde VALUES ('Monika')")
            spark.sql("INSERT INTO test.path_csvserde VALUES ('Buba')")

            val df = spark
              .table("test.path_csvserde")
              .withColumn("ymd", lit(20190401))

            val (plan, _) = lineageCaptor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_csvserde")
            }
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_csvserde")
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_csvserde")
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

          })
        }
      }

    "ParquetSerdeTable" should "Produce CatalogTable params" in
      withRestartingSparkContext {
        withCustomSparkSession(_.enableHiveSupport()) { spark =>
          withLineageTracking(spark)(lineageCaptor => {

            spark.sql("CREATE DATABASE if not exists test")
            spark.sql("CREATE TABLE if not exists test.path_archive_parquetserde (x String, ymd int) " +
              "stored as PARQUET ")
            spark.sql("INSERT INTO test.path_archive_parquetserde VALUES ('Tata', 20190401)")
            spark.sql("INSERT INTO test.path_archive_parquetserde VALUES ('Tere', 20190403)")

            spark.sql("CREATE TABLE if not exists test.path_parquetserde (x String) " +
              "stored as PARQUET ")
            spark.sql("INSERT INTO test.path_parquetserde VALUES ('Monika')")
            spark.sql("INSERT INTO test.path_parquetserde VALUES ('Buba')")

            val df = spark
              .table("test.path_parquetserde")
              .withColumn("ymd", lit(20190401))

            val (plan, _) = lineageCaptor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_parquetserde")
            }
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_parquetserde")
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_parquetserde")
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

          })
        }
      }

    "OrcSerdeTable" should "Produce CatalogTable params" in
      withRestartingSparkContext {
        withCustomSparkSession(_.enableHiveSupport()) { spark =>
          withLineageTracking(spark)(lineageCaptor => {

            spark.sql("CREATE DATABASE if not exists test")
            spark.sql("CREATE TABLE if not exists test.path_archive_orcserde (x String, ymd int) " +
              "stored as orc ")
            spark.sql("INSERT INTO test.path_archive_orcserde VALUES ('Tata', 20190401)")
            spark.sql("INSERT INTO test.path_archive_orcserde VALUES ('Tere', 20190403)")

            spark.sql("CREATE TABLE if not exists test.path_orcserde (x String) " +
              "stored as orc ")
            spark.sql("INSERT INTO test.path_orcserde VALUES ('Monika')")
            spark.sql("INSERT INTO test.path_orcserde VALUES ('Buba')")

            val df = spark
              .table("test.path_orcserde")
              .withColumn("ymd", lit(20190401))

            val (plan, _) = lineageCaptor.lineageOf {
              df.write.mode(SaveMode.Overwrite).insertInto("test.path_archive_orcserde")
            }
            plan.operations.write.outputSource should include("path_archive")
            plan.operations.write.append should be(false)
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_archive_orcserde")
            plan.operations.write.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.table should be("path_orcserde")
            plan.operations.reads.orNull.head.params.get.get("table").orNull.asInstanceOf[CatalogTable].identifier.database should be(Some("test"))

          })
        }
      }
}
