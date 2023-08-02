/*
 * Copyright 2022 ABSA Group Limited
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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.scalatest.ConditionalTestTags.ignoreIf
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.commons.version.Version._
import za.co.absa.spline.test.LineageWalker
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{JDBCFixture, SparkDatabaseFixture, SparkFixture}

import java.util.Properties

class RddSpec extends AsyncFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture
  with SparkDatabaseFixture
  with JDBCFixture {

  private val baseTempDir = TempDirectory().deleteOnExit()
  private val inputPath = baseTempDir.asString
  private val inputPathURI = baseTempDir.path.toUri
  private val tempPath = TempDirectory().deleteOnExit().asString

  it should "support parquet read" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { lineageCaptor =>
        import spark.implicits._

        val testData = {
          Seq((1014, "Warsaw"), (1002, "Corte")).toDF("ID", "NAME")
        }

        for {
          (plan1, Seq(event1)) <- lineageCaptor.lineageOf {
            testData.write.format("parquet").mode("overwrite").save(inputPath)
          }
          (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
            val rddData = spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .parquet(inputPath)
              .select(col("ID").cast("int"))
              .rdd

            val rddRes = rddData.map(row => row.getInt(0) * 2)

            rddRes
              .toDF("foo")
              .write
              .mode(SaveMode.Overwrite)
              .parquet(tempPath)
          }
        } yield {
          plan2.operations.reads.head.inputSources.head should startWith(inputPathURI.toString)
          plan2.operations.write.extra("destinationType") shouldBe Some("parquet")
        }
      }
    }

  it should "support json read" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { lineageCaptor =>
        for {
          (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
            val rddData = spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .json("data/cities.jsonl")
              .rdd

            val schema = StructType(Array(StructField("n", StringType), StructField("p", LongType)))
            spark.createDataFrame(rddData, schema)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(tempPath)
          }
        } yield {
          plan2.operations.reads.head.inputSources.head should startWith("file:/")
          plan2.operations.reads.head.inputSources.head should endWith("data/cities.jsonl")
        }
      }
    }

  it should "support csv read" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { lineageCaptor =>
        for {
          (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
            val schema = StructType(Array(StructField("name", StringType), StructField("population", LongType)))

            val rddData = spark.read
              .option("header", "true")
              .schema(schema)
              .csv("data/cities.csv")
              .rdd

            spark.createDataFrame(rddData, schema)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(tempPath)
          }
        } yield {
          plan2.operations.reads.head.inputSources.head should startWith("file:/")
          plan2.operations.reads.head.inputSources.head should endWith("data/cities.csv")
        }
      }
    }


  it should "support rdd json read" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { lineageCaptor =>
        for {
          (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
            val rddData = spark
              .sparkContext.textFile("data/cities.csv")
              .map(line => Row.fromSeq(line.split(",").take(2)))

            val schema = StructType(Array(StructField("n", StringType), StructField("p", StringType)))
            spark.createDataFrame(rddData, schema)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(tempPath)
          }
        } yield {
          plan2.operations.reads.head.inputSources.head should startWith("file:/")
          plan2.operations.reads.head.inputSources.head should endWith("data/cities.csv")
        }
      }
    }

  it should "support rdd direct data" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { lineageCaptor =>
        for {
          (plan2, Seq(event2)) <- lineageCaptor.lineageOf {
            val rddData = spark
              .sparkContext.parallelize(Array(1, 2, 3, 4))
              .map(Row(_))

            val schema = StructType(Array(StructField("n", IntegerType)))
            spark.createDataFrame(rddData, schema)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(tempPath)
          }
        } yield {
          import za.co.absa.spline.test.ProducerModelImplicits._
          implicit val lineageWalker: LineageWalker = LineageWalker(plan2)

          plan2.operations.reads should be(Seq.empty)

          val write = plan2.operations.write
          val (readLeaves, dataLeaves) = write.dagLeaves

          readLeaves.size should be(0)
          dataLeaves.size should be(1)

          dataLeaves.head.name should be("ParallelCollectionRDD")
        }
      }
    }

  it should "support JDBC as a source" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession { implicit spark =>
      withLineageTracking { captor =>
        val tableName = s"someTable${System.currentTimeMillis()}"

        val schema = StructType(StructField("ID", IntegerType, nullable = false) :: StructField("NAME", StringType, nullable = false) :: Nil)

        val testData: DataFrame = {
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }
        for {
          (plan1, _) <- captor.lineageOf(
            testData
              .write.jdbc(jdbcConnectionString, tableName, new Properties))

          (plan2, _) <- captor.lineageOf {
            val rddData = spark
              .read.jdbc(jdbcConnectionString, tableName, new Properties)
              .rdd

            spark.createDataFrame(rddData, schema)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(tempPath)
          }
        } yield {
          plan2.operations.reads.head.inputSources.head shouldBe s"$jdbcConnectionString:$tableName"
          plan2.operations.reads.head.extra("sourceType") shouldBe Some("jdbc")
        }
      }
    }
}
