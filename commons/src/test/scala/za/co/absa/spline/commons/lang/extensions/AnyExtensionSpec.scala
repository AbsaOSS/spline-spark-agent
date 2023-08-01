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

import org.mockito.Mockito
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class AnyExtensionSpec extends AnyFunSpec with Matchers with MockitoSugar {

  import AnyExtension._

  describe("AnyOps") {

    describe("optionally()") {

      it("should do nothing when maybeArg is None") {
        val aString = "abc"
        val aInt = 1
        val aList = List(1, 2, 3)

        aString.optionally[Float]((s, b) => b.toString, None) shouldEqual aString
        aInt.optionally[Float]((i, b) => b.toInt, None) shouldEqual aInt
        aList.optionally[Float]((l, b) => List(b.toInt), None) shouldEqual aList
      }

      it("should not call applyFn when maybeArg is None") {
        val aString = "abc"
        val applyFn = mock[(String, Float) => String]

        aString.optionally[Float](applyFn, None)

        Mockito.verifyNoInteractions(applyFn)
      }

      it("should return result of applied applyFn when maybeArg is not None") {
        val aString = "abc"
        val aInt = 1
        val aList = List(1, 2, 3)

        aString.optionally[Float]((s, b) => b.toString, Some(0.0F)) shouldEqual "0.0"
        aInt.optionally[Float]((i, b) => b.toInt, Some(0.0F)) shouldEqual 0
        aList.optionally[Float]((l, b) => List(b.toInt), Some(5.0F)) shouldEqual List(5)
      }
    }

    describe("when()") {

      it("should only call a method when `cond` is true") {
        val neverFn = mock[Int => Int]

        (1 when true)(_ + 2) should be(3)
        (1 when false)(neverFn) should be(1)

        Mockito.verifyNoInteractions(neverFn)
      }
    }

    describe("unless()") {

      it("should only call a method when `cond` is false") {
        val neverFn = mock[Int => Int]

        (1 unless false)(_ + 2) should be(3)
        (1 unless true)(neverFn) should be(1)
        Mockito.verifyNoInteractions(neverFn)
      }
    }
  }

}
