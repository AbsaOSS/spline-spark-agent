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

package za.co.absa.spline.commons.lang.extensions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.commons.lang.extensions.OptionExtension.OptionOps

import scala.util.{Failure, Success}

class OptionExtensionSpec extends AnyFunSuite with Matchers {

  test("OptionOps.toTry - case Some") {
    val option = Some("abc")
    val expected = Success("abc")

    val actual = option.toTry(new Exception)

    assert(expected == actual)
  }

  test("OptionOps.toTry - case None") {
    val option = None

    val result = option.toTry(new Exception("exception occurred"))

    assert(result.isFailure)
    assert(result match { case Failure(e) => e.getMessage == "exception occurred" })
  }

}
