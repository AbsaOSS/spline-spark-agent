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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.commons.scalatest.ConditionalTestTags._
import za.co.absa.commons.version.Version._
class FileFormatSpec extends AnyFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture {

  private val avroPath = TempDirectory(prefix = "avro", pathOnly = true).deleteOnExit().path.toFile.getAbsolutePath
  private val jsonPath = TempDirectory(prefix = "json", pathOnly = true).deleteOnExit().path.toFile.getAbsolutePath
  private val orcPath = TempDirectory(prefix = "orc", pathOnly = true).deleteOnExit().path.toFile.getAbsolutePath
  private val csvPath = TempDirectory(prefix = "csv", pathOnly = true).deleteOnExit().path.toFile.getAbsolutePath

  //Spark supports avro out of the box in 2.4 and beyond
  it should "support built in avro as a source" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.4.0") in
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {
        val testData: DataFrame = {
          val schema = StructType(StructField("ID", IntegerType, nullable = false) :: StructField("NAME", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }

        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write.format("avro").mode("overwrite").save(avroPath))

        val (plan2, _) = lineageCaptor.lineageOf(spark
          .read.format("avro").load(avroPath)
          .write.mode(Overwrite).saveAsTable("somewhere")
        )

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType") shouldBe Some("Avro")
        plan1.operations.write.outputSource shouldBe s"file:$avroPath"
        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType") shouldBe Some("Avro")
      })
    })

  //users would have to use the databricks library for avro prior to spark 2.4
  it should "support databricks avro as a source" taggedAs ignoreIf(ver"$SPARK_VERSION" >= ver"2.4.0") in
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {
        val testData: DataFrame = {
          val schema = StructType(StructField("ID", IntegerType, nullable = false) :: StructField("NAME", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }

        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write.format("com.databricks.spark.avro").mode("overwrite").save(avroPath))

        val (plan2, _) = lineageCaptor.lineageOf(spark
          .read.format("com.databricks.spark.avro").load(avroPath)
          .write.mode(Overwrite).saveAsTable("somewhere")
        )

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType") shouldBe Some("Avro")
        plan1.operations.write.outputSource shouldBe s"file:$avroPath"
        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType") shouldBe Some("Avro")
      })
    })

  /* "json" is returned as the destination/source type in spark 2.2 and "JSON" in all newer versions
  Performing upper case compare for validation purposes
   */
  it should "support json as a source" in
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {
        val testData: DataFrame = {
          val schema = StructType(StructField("ID", IntegerType, nullable = false) :: StructField("NAME", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }

        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write.format("json").mode("overwrite").save(jsonPath))

        val (plan2, _) = lineageCaptor.lineageOf(spark
          .read.format("json").load(jsonPath)
          .write.mode(Overwrite).saveAsTable("somewhere")
        )

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType").asInstanceOf[Some[String]].get.toString.toUpperCase() shouldBe "JSON"
        plan1.operations.write.outputSource shouldBe s"file:$jsonPath"
        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType").asInstanceOf[Some[String]].get.toString.toUpperCase() shouldBe "JSON"
      })
    })

  /* "orc" is returned as the destination/source type in spark 2.2 and "ORC" in all newer versions
  Performing upper case compare for validation purposes
   */
  it should "support orc as a source" in
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {
        val testData: DataFrame = {
          val schema = StructType(StructField("ID", IntegerType, nullable = false) :: StructField("NAME", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }

        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write.format("orc").mode("overwrite").save(orcPath))

        val (plan2, _) = lineageCaptor.lineageOf(spark
          .read.format("orc").load(orcPath)
          .write.mode(Overwrite).saveAsTable("somewhere")
        )

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType").asInstanceOf[Some[String]].get.toString.toUpperCase() shouldBe "ORC"
        plan1.operations.write.outputSource shouldBe s"file:$orcPath"
        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType").asInstanceOf[Some[String]].get.toString.toUpperCase() shouldBe "ORC"
      })
    })

  /* "csv" is returned as the destination/source type in spark 2.2 and "CSV" in all newer versions
  Performing upper case compare for validation purposes
   */
  it should "support csv as a source" in
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {
        val testData: DataFrame = {
          val schema = StructType(StructField("ID", IntegerType, nullable = false) :: StructField("NAME", StringType, nullable = false) :: Nil)
          val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
          spark.sqlContext.createDataFrame(rdd, schema)
        }

        val (plan1, _) = lineageCaptor.lineageOf(testData
          .write.format("csv").mode("overwrite").save(csvPath))

        val (plan2, _) = lineageCaptor.lineageOf(spark
          .read.format("csv").load(csvPath)
          .write.mode(Overwrite).saveAsTable("somewhere")
        )

        plan1.operations.write.append shouldBe false
        plan1.operations.write.extra.get("destinationType").asInstanceOf[Some[String]].get.toString.toUpperCase() shouldBe "CSV"
        plan1.operations.write.outputSource shouldBe s"file:$csvPath"
        plan2.operations.reads.get.head.inputSources.head shouldBe plan1.operations.write.outputSource
        plan2.operations.reads.get.head.extra.get("sourceType").asInstanceOf[Some[String]].get.toString.toUpperCase() shouldBe "CSV"
      })
    })
}


