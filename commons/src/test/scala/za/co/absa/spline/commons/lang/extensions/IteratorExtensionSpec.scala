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

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class IteratorExtensionSpec extends AnyFunSpec with Matchers with MockitoSugar {

  import IteratorExtension._

  describe("IteratorOps") {

    describe("fetchToArray()") {

      it("should fill the array") {
        val arr = Array.ofDim[Char](5)
        val iter = "abcdefghijklmnopqrstuvwxyz".iterator

        val cnt = iter.fetchToArray(arr, 0, 5)

        cnt should be(5)
        arr shouldEqual "abcde".toArray
        iter should have length 21
      }

      it("should copy a range of items into an array") {
        val arr = Array.ofDim[Char](5)
        val iter = "abcdefghijklmnopqrstuvwxyz".iterator

        val cnt = iter.fetchToArray(arr, 1, 3)

        cnt should be(3)
        arr shouldEqual Array(0, 'a', 'b', 'c', 0)
        iter should have length 23
      }

      it("should exhaust the iterator if one has not enough items") {
        val arr = Array.ofDim[Char](5)
        val iter = "a".iterator

        val cnt = iter.fetchToArray(arr, 1, 3)

        cnt should be(1)
        arr shouldEqual Array(0, 'a', 0, 0, 0)
        iter should be(empty)
      }

      it("should not overfill the array") {
        val arr = Array.ofDim[Char](5)
        val iter = "abcdefghijklmnopqrstuvwxyz".iterator

        val cnt = iter.fetchToArray(arr, 1, 99)

        cnt should be(4)
        arr shouldEqual Array(0, 'a', 'b', 'c', 'd')
        iter should have length 22
      }

      it("should do nothing on empty range") {
        val arr = Array.ofDim[Char](5)
        val iter = "abcdefghijklmnopqrstuvwxyz".iterator

        val cnt = iter.fetchToArray(arr, 1, 0)

        cnt should be(0)
        all(arr) should be(0)
        iter should have length 26
      }

      it("should do nothing on empty iterators") {
        val arr = Array.ofDim[Char](5)
        val cnt = Iterator[Char]().fetchToArray(arr, 1, 3)
        cnt should be(0)
        all(arr) should be(0)
      }

      it("should throw on incorrect array bounds") {
        val arr = Array.ofDim[Char](5)
        intercept[IllegalArgumentException](Iterator[Char]().fetchToArray(arr, 5, 0))
        intercept[IllegalArgumentException](Iterator[Char]().fetchToArray(arr, -1, 0))
      }
    }

  }

}
