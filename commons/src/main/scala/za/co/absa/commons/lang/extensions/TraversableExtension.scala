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

object TraversableExtension {

  implicit class TraversableOps[A <: Traversable[_]](val xs: A) extends AnyVal {

    /**
     * @return None if Traversable is null or is empty, otherwise returns Some(traversable).
     */
    @deprecated("Use toNonEmptyOption instead")
    def asOption: Option[A] = if (xs == null || xs.isEmpty) None else Some(xs)

    /**
     * Converts Traversable to non empty option.
     *
     * Use method from [[za.co.absa.commons.lang.extensions.NonOptionExtension]] to just wrap the Traversable in Option
     * without the emptiness check.
     *
     * @return None if Traversable is null or is empty, otherwise returns Some(traversable).
     */
    def toNonEmptyOption: Option[A] = if (xs == null || xs.isEmpty) None else Some(xs)

  }

}
