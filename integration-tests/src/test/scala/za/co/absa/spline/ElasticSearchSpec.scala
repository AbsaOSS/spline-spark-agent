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

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.elasticsearch.ElasticsearchContainer
import za.co.absa.spline.ElasticSearchSpec.Formats
import za.co.absa.spline.commons.io.TempDirectory
import za.co.absa.spline.test.fixture.spline.SplineFixture
import za.co.absa.spline.test.fixture.{ReleasableResourceFixture, SparkFixture}


object ElasticSearchSpec {
  private val Formats = Seq("es", "org.elasticsearch.spark.sql")
}

class ElasticSearchSpec
  extends AsyncFlatSpec
    with Matchers
    with SparkFixture
    with SplineFixture
    with ReleasableResourceFixture {

  for (format <- Formats) it should s"""support `format("$format")` with `save/load("<index>")`""" in {
    usingResource(new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.9.2")) { container =>
      container.start()

      val index = "test"
      val docType = "test"
      val esNodes = container.getHost
      val esPort = container.getFirstMappedPort
      val esOptions = Map(
        "es.nodes" -> esNodes,
        "es.port" -> esPort.toString,
        "es.nodes.wan.only" -> "true"
      )

      withNewSparkSession(implicit spark => {
        withLineageTracking { captor =>
          val testData: DataFrame = {
            val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
            val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
            spark.sqlContext.createDataFrame(rdd, schema)
          }

          for {
            (plan1, _) <- captor.lineageOf {
              testData
                .write
                .mode(SaveMode.Append)
                .options(esOptions)
                .format(format)
                .save(s"$index/$docType")
            }

            (plan2, _) <- captor.lineageOf {
              val df = spark
                .read
                .options(esOptions)
                .format(format)
                .load(s"$index/$docType")

              df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
            }
          } yield {

            plan1.operations.write.append shouldBe true
            plan1.operations.write.extra("destinationType") shouldBe Some("elasticsearch")
            plan1.operations.write.outputSource shouldBe s"elasticsearch://$esNodes/$index/$docType"

            plan2.operations.reads.head.inputSources.head shouldBe plan1.operations.write.outputSource
            plan2.operations.reads.head.extra("sourceType") shouldBe Some("elasticsearch")
            plan2.operations.write.append shouldBe false
          }
        }
      })
    }
  }

  for (format <- Formats) it should s"""support `format("$format")` with `option("es.resource", "<index>")`""" in {
    usingResource(new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.9.2")) { container =>
      container.start()

      val index = "test"
      val docType = "test"
      val esNodes = container.getHost
      val esPort = container.getFirstMappedPort
      val esOptions = Map(
        "es.nodes" -> esNodes,
        "es.port" -> esPort.toString,
        "es.nodes.wan.only" -> "true",
        "es.resource" -> s"$index/$docType"
      )

      withNewSparkSession(implicit spark => {
        withLineageTracking { captor =>
          val testData: DataFrame = {
            val schema = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable = false) :: Nil)
            val rdd = spark.sparkContext.parallelize(Row(1014, "Warsaw") :: Row(1002, "Corte") :: Nil)
            spark.sqlContext.createDataFrame(rdd, schema)
          }

          for {
            (plan1, _) <- captor.lineageOf {
              testData
                .write
                .mode(SaveMode.Append)
                .options(esOptions)
                .format(format)
                .save()
            }

            (plan2, _) <- captor.lineageOf {
              val df = spark
                .read
                .options(esOptions)
                .format(format)
                .load()

              df.write.save(TempDirectory(pathOnly = true).deleteOnExit().path.toString)
            }
          } yield {

            plan1.operations.write.append shouldBe true
            plan1.operations.write.extra("destinationType") shouldBe Some("elasticsearch")
            plan1.operations.write.outputSource shouldBe s"elasticsearch://$esNodes/$index/$docType"

            plan2.operations.reads.head.inputSources.head shouldBe plan1.operations.write.outputSource
            plan2.operations.reads.head.extra("sourceType") shouldBe Some("elasticsearch")
            plan2.operations.write.append shouldBe false
          }
        }
      })
    }
  }
}
