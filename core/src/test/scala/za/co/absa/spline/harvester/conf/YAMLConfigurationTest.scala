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
      |""".stripMargin)

  test("null keys should be mapped to their parent") {
    givenConf.containsKey("spline") shouldBe true
    givenConf.getKeys("spline").asScala.toSeq should contain("spline")
    givenConf.getProperty("spline") shouldEqual "null_key"
  }

  test("null values should be completely ignored") {
    givenConf.containsKey("spline.null_value") shouldBe false
    givenConf.getKeys("spline").asScala.toSeq shouldNot contain("spline.null_value")
    givenConf.getProperty("spline.null_value") shouldEqual null
  }
}
