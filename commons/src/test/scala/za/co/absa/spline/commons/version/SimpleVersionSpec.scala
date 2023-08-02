/*
 * Copyright 2023 ABSA Group Limited
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

package za.co.absa.spline.commons.version

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.commons.version.Version._

import scala.Ordering.Implicits._
import scala.util.Random

class SimpleVersionSpec extends AnyFlatSpec with Matchers {

  behavior of "Version parser"

  it should "parse simple version" in {
    Version.asSimple("1.two.33.forty-two") should be(Version(
      NumericComponent(1),
      StringComponent("two"),
      NumericComponent(33),
      StringComponent("forty-two")))
  }

  // bugfix: commons-14
  it should "handle big numbers" in {
    val bigStr = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
    Version.asSimple(bigStr) should be(Version(NumericComponent(BigInt(bigStr))))
  }

  it should "not parse" in {
    intercept[IllegalArgumentException](Version.asSimple(""))
  }

  behavior of "Version ordering"

  it should "compare" in {
    (ver"1.2" equiv ver"01.002") should be(true)
    (ver"0" < ver"0.0.0") should be(true)
    (ver"1.1" < ver"1.1.0") should be(true)
    (ver"1.22" > ver"0.22") should be(true)
    (ver"1.22" > ver"1.21") should be(true)
    (ver"1.22" > ver"1.9") should be(true)
    (ver"1.22" > ver"1.9.9999") should be(true)
    (ver"1.22" > ver"1.21.9999") should be(true)
    (ver"1.22" < ver"1.22.0.1") should be(true)
    (ver"1.22" < ver"1.111") should be(true)
  }

  it should "sort" in {
    val versions = Seq(
      ver"0.22",
      ver"1.9",
      ver"1.9.9999",
      ver"1.21",
      ver"1.21.9999",
      ver"1.22.0.1",
      ver"1.111"
    )
    Random.shuffle(versions).sorted should equal(versions)
  }

  behavior of "asString()"

  it should "render the original version string" in {
    ver"1".asString should be("1")
    ver"foo".asString should be("foo")
    ver"1.two.33.forty-two".asString should be("1.two.33.forty-two")
  }
}
