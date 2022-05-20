/*
 * Copyright 2021 ABSA Group Limited
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

import org.scalatest.TryValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.CollectionImplicits._

class CollectionImplicitsSpec
  extends AnyFlatSpec
    with Matchers {

  behavior of "MapOps.|+|"

  it should "merge two maps with append" in {
    val m1 = Map("a" -> 1, "b" -> 1)
    val m2 = Map("b" -> 1, "c" -> 1)
    (m1 |+| m2) should equal(Map("a" -> 1, "b" -> 2, "c" -> 1))
  }

  it should "ignore empty maps" in {
    (Map.empty[String, Int] |+| Map.empty[String, Int]) should be(empty)
    (Map.empty[String, Int] |+| Map("a" -> 1)) should equal(Map("a" -> 1))
    (Map("a" -> 1) |+| Map.empty[String, Int]) should equal(Map("a" -> 1))
  }

  behavior of "SeqOps.tryReduce"

  it should "require non-empty input" in {
    an[UnsupportedOperationException] should be thrownBy Nil.tryReduce(identity[Nothing])
  }

  it should "map a sequence on a given function and wrap a result in Try.Success" in {
    Seq(1, 2, 3).tryReduce(2./).success.value shouldEqual Seq(2, 1, 2 / 3)
  }

  it should "skip items on which a mapping function fails" in {
    Seq(0, 1, 2).tryReduce(2./).success.value shouldEqual Seq(2, 1) // 2 divided by 0 fails and is skipped
    Seq(-1, 0, 1).tryReduce(2./).success.value shouldEqual Seq(-2, 2) // 2 divided by 0 fails and is skipped
    Seq(-2, -1, 0).tryReduce(2./).success.value shouldEqual Seq(-1, -2) // 2 divided by 0 fails and is skipped
  }

  it should "return the first Try.Failure if a function failed on all items" in {
    Seq(1).tryReduce(_ => sys.error(s"the error")).failure.exception.getMessage shouldEqual "the error"
    Seq(1, 2).tryReduce(i => sys.error(s"meh $i")).failure.exception.getMessage shouldEqual "meh 1"
  }
}
