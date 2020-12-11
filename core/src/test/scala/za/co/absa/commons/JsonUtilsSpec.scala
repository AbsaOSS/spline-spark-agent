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

package za.co.absa.commons

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonUtilsSpec extends AnyFlatSpec with Matchers {
  "prettifyJson()" should "prettify json" in {
    val compactJson = """{"a":42,"b":{"c":111,"d":222},"x":[1,2]}"""
    val prettyJson = JsonUtils.prettifyJson(compactJson)

    prettyJson should equal(
      """{
        |  "a" : 42,
        |  "b" : {
        |    "c" : 111,
        |    "d" : 222
        |  },
        |  "x" : [ 1, 2 ]
        |}""".stripMargin)
  }
}
