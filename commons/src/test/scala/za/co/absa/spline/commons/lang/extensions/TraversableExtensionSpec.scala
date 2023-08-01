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

class TraversableExtensionSpec extends AnyFunSpec with Matchers with MockitoSugar {

  import TraversableExtension._

  describe("TraversableOps") {
    describe("toNonEmptyOption()") {

      it("should return None if the traversable is null") {
        val traversable: Traversable[Int] = null

        traversable.toNonEmptyOption shouldEqual None
      }

      it("should return None if the traversable is empty") {
        val traversable: Traversable[Int] = List[Int]()

        traversable.toNonEmptyOption shouldEqual None
      }

      it("should return Some(traversable) if the traversable is not empty") {
        val traversable: Traversable[Int] = List[Int](1, 2, 3)

        traversable.toNonEmptyOption shouldEqual Some(traversable)
      }
    }

    describe("asOption()") {

      it("should return None if the traversable is null") {
        val traversable: Traversable[Int] = null

        traversable.asOption shouldEqual None
      }

      it("should return None if the traversable is empty") {
        val traversable: Traversable[Int] = List[Int]()

        traversable.asOption shouldEqual None
      }

      it("should return Some(traversable) if the traversable is not empty") {
        val traversable: Traversable[Int] = List[Int](1, 2, 3)

        traversable.asOption shouldEqual Some(traversable)
      }
    }
  }

}
