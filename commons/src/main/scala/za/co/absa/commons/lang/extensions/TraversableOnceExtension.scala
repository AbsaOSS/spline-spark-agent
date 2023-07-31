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

package za.co.absa.commons.lang.extensions

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

object TraversableOnceExtension {

  implicit class TraversableOnceOps[A, M[X] <: TraversableOnce[X]](val xs: M[A]) extends AnyVal {

    /**
     * Almost like `distinct`, but instead of comparing elements itself it compares their projections,
     * returned by a given function `f`.
     *
     * It's logically equivalent to doing stable grouping by `f` followed by selecting the first value of each key.
     *
     * @param f   projection function
     * @param cbf collection builder factory
     * @return a new sequence of elements which projection (obtained by applying the function `f`)
     *         are the first occurrence of every other elements' projection of this sequence.
     */
    def distinctBy[B](f: A => B)(implicit cbf: CanBuildFrom[M[A], A, M[A]]): M[A] = {
      val seen = mutable.Set.empty[B]
      val b = cbf(xs)
      for (x <- xs) {
        val y = f(x)
        if (!seen(y)) {
          b += x
          seen += y
        }
      }
      b.result()
    }

  }

}
