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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import za.co.absa.commons.io.TempFile
import za.co.absa.spline.SparkApp

object UDFJob extends SparkApp(name = "UDFJob") {
  val path = TempFile("udf-file", ".csv", pathOnly = false).deleteOnExit().path

  import za.co.absa.spline.harvester.SparkLineageInitializer._

  // Initializing library to hook up to Apache Spark
  spark.enableLineageTracking()

  val userData = spark.createDataFrame(Seq(
    (1, "Chandler", "Pasadena", "US"),
    (2, "Monica", "New york", "USa"),
    (3, "Phoebe", "Suny", "USA"),
    (4, "Rachael", "St louis", "United states of America"),
    (5, "Joey", "LA", "Ussaa"),
    (6, "Ross", "Detroit", "United states")
  )).toDF("id", "name", "city", "country")

  def cleanCountry = (country: String) => {
    val allUSA = Seq("US", "USa", "USA", "United states", "United states of America")
    if (allUSA.contains(country)) {
      "USA"
    }
    else {
      "unknown"
    }
  }

  val normaliseCountry = spark.udf.register("normalisedCountry", cleanCountry)

  userData
    .withColumn("normalisedCountry", normaliseCountry(col("country")))
    .write.mode(SaveMode.Overwrite).csv(path.toString)
}
