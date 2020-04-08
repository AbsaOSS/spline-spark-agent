/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.conf

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.spline.harvester.conf.StandardSplineConfigurationStackSpec._

class StandardSplineConfigurationStackSpec extends AnyFlatSpec with Matchers with SparkTestBase {
  behavior of "StandardSplineConfigurationStack"

  it should "look through the multiple sources for the configuration properties" in {
    val jvmProps = System.getProperties
    val sparkConf = (classOf[SparkContext] getMethod "conf" invoke spark.sparkContext).asInstanceOf[SparkConf]
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    for (key <- Some(KeyDefinedEverywhere)) {
      jvmProps.setProperty(key, ValueFromJVM)
      hadoopConf.set(key, ValueFromHadoop)
      sparkConf.set(s"spark.$key", ValueFromSpark)
      // skip setting spline prop as it's already hardcoded in spline.properties
    }

    for (key <- Some(KeyDefinedInJVMAndSpline)) {
      jvmProps.setProperty(key, ValueFromJVM)
      // skip setting spline prop as it's already hardcoded in spline.properties
    }

    for (key <- Some(KeyDefinedInHadoopAndSpline)) {
      hadoopConf.set(key, ValueFromHadoop)
      // skip setting spline prop as it's already hardcoded in spline.properties
    }

    for (key <- Some(KeyDefinedInHadoopAndSpark)) {
      hadoopConf.set(key, ValueFromHadoop)
      sparkConf.set(s"spark.$key", ValueFromSpark)
    }

    for (key <- Some(KeyDefinedInSparkAndSpline)) {
      sparkConf.set(s"spark.$key", ValueFromSpark)
      // skip setting spline prop as it's already hardcoded in spline.properties
    }

    val splineConfiguration = StandardSplineConfigurationStack(spark)

    splineConfiguration getString KeyDefinedEverywhere shouldEqual ValueFromHadoop
    splineConfiguration getString KeyDefinedInJVMAndSpline shouldEqual ValueFromJVM
    splineConfiguration getString KeyDefinedInHadoopAndSpline shouldEqual ValueFromHadoop
    splineConfiguration getString KeyDefinedInHadoopAndSpark shouldEqual ValueFromHadoop
    splineConfiguration getString KeyDefinedInSparkAndSpline shouldEqual ValueFromSpark
    splineConfiguration getString KeyDefinedInSplineOnly shouldEqual ValueFromSpline
    splineConfiguration getString "key.undefined" shouldBe null
  }
}

object StandardSplineConfigurationStackSpec {
  val KeyDefinedEverywhere = "key.defined.everywhere"
  val KeyDefinedInHadoopAndSpline = "key.defined.in_Hadoop_and_Spline"
  val KeyDefinedInHadoopAndSpark = "key.defined.in_Hadoop_and_Spark"
  val KeyDefinedInSparkAndSpline = "key.defined.in_Spark_and_Spline"
  val KeyDefinedInJVMAndSpline = "key.defined.in_JVM_and_Spline"
  val KeyDefinedInSplineOnly = "key.defined.in_Spline_only"

  val ValueFromHadoop = "value from Hadoop configuration"
  val ValueFromSpark = "value from Spark configuration"
  val ValueFromJVM = "value from JVM args"
  val ValueFromSpline = "value from spline.properties"
}
