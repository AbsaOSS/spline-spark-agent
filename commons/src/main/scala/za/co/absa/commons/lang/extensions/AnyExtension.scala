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

object AnyExtension {

  implicit class AnyOps[A <: Any](val a: A) extends AnyVal {

    /**
     * Applies `applyFn` to the value with `maybeArg` as second argument in case `maybeArg` is not None,
     * otherwise does nothing and returns the same value as was called on.
     */
    def optionally[B](applyFn: (A, B) => A, maybeArg: Option[B]): A = maybeArg.map(applyFn(a, _)).getOrElse(a)

    /**
     * The same as [[optionally]], but with signature that is more type inference friendly.
     */
    def having[B](maybeArg: Option[B])(applyFn: (A, B) => A): A = optionally(applyFn, maybeArg)

    /**
     * Conditionally apply the given function on a decoratee object, WHEN the given condition holds.
     *
     * @param cond    a boolean value indicating if the condition holds
     * @param applyFn a function to apply
     * @return when a given condition is true, apply the function and return its result, otherwise returns the original decoratee object
     */
    def when(cond: Boolean)(applyFn: A => A): A = if (cond) applyFn(a) else a

    /**
     * Logically opposite to the `when()`.
     * Conditionally apply the given function on a decoratee object, UNLESS the given condition holds.
     *
     * @param cond    a boolean value indicating if the condition holds
     * @param applyFn a function to apply
     * @return when a given condition is true, apply the function and return its result, otherwise returns the original decoratee object
     */
    def unless(cond: Boolean)(applyFn: A => A): A = when(!cond)(applyFn)
  }

}
