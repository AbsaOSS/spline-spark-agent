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

import scala.collection.mutable

class TraversableOnceExtensionSpec extends AnyFunSpec with Matchers with MockitoSugar {

  import TraversableOnceExtension._

  describe("TraversableOnceOps") {
    describe("distinctBy()") {

      it("should do nothing on empty immutable collections") {
        val dummyFn = mock[Unit => _]
        Seq.empty[Unit].distinctBy(dummyFn) should be theSameInstanceAs Seq.empty
        Mockito.verifyNoInteractions(dummyFn)
      }

      it("should create a new instance of empty mutable collections") {
        val dummyFn = mock[Unit => _]
        val originalCol = mutable.Seq.empty[Unit]
        val distinctCol = originalCol.distinctBy(dummyFn)

        distinctCol should not(be theSameInstanceAs originalCol)
        distinctCol should have length 0
        Mockito.verifyNoInteractions(dummyFn)
      }

      it("should return the same collection type as it was called on") {
        // immutable
        Seq(1, 2).distinctBy(identity) should be(a[Seq[_]])
        List(1, 2).distinctBy(identity) should be(a[List[_]])
        Vector(1, 2).distinctBy(identity) should be(a[Vector[_]])
        Set(1, 2).distinctBy(identity) should be(a[Set[_]])
        // mutable
        mutable.Seq(1, 2).distinctBy(identity) should be(a[mutable.Seq[_]])
        mutable.ListBuffer(1, 2).distinctBy(identity) should be(a[mutable.ListBuffer[_]])
        mutable.HashSet(1, 2).distinctBy(identity) should be(a[mutable.HashSet[_]])
      }

      it("should remove entries with duplicated projection") {
        val abcde: Seq[(String, Int)] = Seq(
          "a" -> 1,
          "b" -> 2,
          "c" -> 2,
          "d" -> 3,
          "e" -> 1
        )

        val abd = abcde.distinctBy { case (_, i) => i }

        abd should be(a[Seq[_]])
        abd should contain theSameElementsInOrderAs Seq(
          "a" -> 1,
          "b" -> 2,
          "d" -> 3
        )
      }
    }
  }

}
