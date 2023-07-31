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

import scala.collection.mutable.ListBuffer

object SeqExtension {
  implicit class SeqOps[T](val seq: Seq[T]) extends AnyVal {

    /**
     * Collects consecutive items into groups if they have the same grouping value.
     * A grouping value is provided by a callback function.
     *
     * It never groups `null` values.
     *
     * It is stable (does not change the original order of the elements).
     *
     * @param f A function that takes element of the sequence and returns its grouping value
     * @return A sequence of groups
     */
    def groupConsecutiveBy[A](f: T => A): Seq[Seq[T]] = {
      var lastElement: Option[A] = None
      var group = new ListBuffer[T]
      val groups = new ListBuffer[List[T]]

      def pushGroup(): Unit = {
        if (group.nonEmpty) {
          groups += group.toList
          group = new ListBuffer[T]
        }
      }

      for (elem <- seq) {
        val curGroupElement = f(elem)
        lastElement match {
          case Some(lastGroupElem) =>
            if (curGroupElement == null || !curGroupElement.equals(lastGroupElem)) {
              pushGroup()
            }
            group += elem
          case None =>
            pushGroup()
            group += elem
        }
        lastElement = Option(curGroupElement)
      }

      pushGroup()
      groups.toList
    }

    /**
     * Collects consecutive items into groups if the predicate for all of them returns `true`.
     * A predicate is provided by a callback function.
     *
     * It is stable (does not change the original order of the elements).
     *
     * @param p A predicate that should return `true` for each element that should be groped.
     * @return A sequence of groups
     */
    def groupConsecutiveByPredicate(p: T => Boolean): Seq[Seq[T]] = {
      val groupingValueFunction = (t: T) => p(t) match {
        case false => NeverEquals
        case true => true
      }
      groupConsecutiveBy(groupingValueFunction)
    }

    /**
     * Collects consecutive items into groups if they have the same grouping value and that value is not None.
     * A grouping value should be provided as an Option[T] by a callback function.
     *
     * It is stable (does not change the original order of the elements).
     *
     * @param f A function that takes element of the sequence and returns its grouping value as an Option
     * @return A sequence of groups
     */
    def groupConsecutiveByOption[A](f: T => Option[A]): Seq[Seq[T]] = {
      val groupingValueFunction = (a: T) => f(a).getOrElse(NeverEquals)
      groupConsecutiveBy(groupingValueFunction)
    }

  }

  private object NeverEquals {
    override def equals(obj: Any): Boolean = false
  }

}
