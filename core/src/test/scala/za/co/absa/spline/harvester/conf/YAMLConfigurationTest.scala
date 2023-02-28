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

package za.co.absa.spline.harvester.conf

import scala.collection.JavaConverters._

class YAMLConfigurationTest extends ReadOnlyConfigurationTest {

  override protected val emptyConf = new YAMLConfiguration("")
  override protected val givenConf = new YAMLConfiguration(
    """
      |spline:
      |  x:
      |    y: foo
      |  w:
      |    z: bar
      |  xs:
      |    - a
      |    - b
      |    - c
      |  ~: null_key
      |  null_value:
      |
      |  # ------------------------------
      |  # for value type sensitive tests
      |  # ------------------------------
      |
      |  decimalAsNumber: 1.2
      |  decimalAsString: "1.2"
      |  integerAsNumber: 42
      |  integerAsString: "42"
      |  booleanAsBoolean: true
      |  booleanAsString: "true"
      |  heterogeneousValues:
      |    - 1.2
      |    - 42
      |    - true
      |    - "foo"
      |""".stripMargin)

  test("null keys should be mapped to their parent") {
    givenConf.containsKey("spline") shouldBe true
    givenConf.getKeys("spline").asScala.toSeq should contain("spline")
    givenConf.getProperty("spline") shouldEqual "null_key"
  }

  test("null values should be completely ignored") {
    givenConf.containsKey("spline.null_value") shouldBe false
    givenConf.getKeys("spline").asScala.toSeq shouldNot contain("spline.null_value")
    givenConf.getProperty("spline.null_value") shouldBe null
  }

  test("get string value as String") {
    givenConf getString "spline.x.y" shouldEqual "foo"
    givenConf getString "spline.w.z" shouldEqual "bar"
  }

  test("get string value as another type") {
    givenConf getDouble "spline.decimalAsString" shouldBe 1.2d
    givenConf getInt "spline.integerAsString" shouldBe 42
    givenConf getBoolean "spline.booleanAsString" shouldBe true
  }

  test("get non-string value as String") {
    givenConf getString "spline.decimalAsNumber" shouldEqual "1.2"
    givenConf getString "spline.integerAsNumber" shouldEqual "42"
    givenConf getString "spline.booleanAsBoolean" shouldEqual "true"
  }

  test("get non-string value as its own type") {
    givenConf getDouble "spline.decimalAsNumber" shouldBe 1.2d
    givenConf getInt "spline.integerAsNumber" shouldBe 42
    givenConf getBoolean "spline.booleanAsBoolean" shouldBe true
  }

  test("get single-valued non-string property as List") {
    givenConf getList "spline.decimalAsNumber" should contain theSameElementsAs Seq("1.2")
    givenConf getList "spline.integerAsNumber" should contain theSameElementsAs Seq("42")
    givenConf getList "spline.booleanAsBoolean" should contain theSameElementsAs Seq("true")
  }

  test("get heterogeneous list") {
    givenConf getList "spline.heterogeneousValues" should contain theSameElementsInOrderAs Seq(1.2, 42, true, "foo")
  }
}
