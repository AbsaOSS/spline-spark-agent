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

class ArrayExtensionSpec extends AnyFunSpec with Matchers with MockitoSugar {

  import ArrayExtension._

  describe("ArrayOps") {
    describe("distinctBy()") {

      it("should create a new instance of empty array when called on empty array") {
        val dummyFn = mock[Unit => _]
        val originalArr = Array.empty[Unit]
        val distinctArr = originalArr.distinctBy(dummyFn)

        distinctArr should not(be theSameInstanceAs originalArr)
        distinctArr should have length 0
        Mockito.verifyNoInteractions(dummyFn)
      }

      it("should eliminate duplicates on identity projection") {
        val arr = Array(1, 2, 1)

        val duplicatesEliminated = arr.distinctBy(identity)

        duplicatesEliminated shouldEqual Array(1, 2)
      }

      it("should eliminate duplicates on custom projection") {
        val arr = Array(1, 2, 1, 0, 3)

        val duplicatesEliminated = arr.distinctBy(x => x % 2)

        duplicatesEliminated shouldEqual Array(1, 2)
      }

    }
  }

}
