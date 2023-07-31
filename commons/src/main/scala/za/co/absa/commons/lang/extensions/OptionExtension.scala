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

import scala.util.{Failure, Success, Try}

object OptionExtension {

  implicit class OptionOps[T](val option: Option[T]) extends AnyVal {

    /**
     * Converts Option to Try such that Some(x) becomes Success(x), and None becomes Failure(exception),
     * where exception is provided as parameter.
     *
     * @param failure an Exception to use in Failure in case Option is None
     * @return Try converted from Option
     */
    def toTry(failure: => Exception): Try[T] =
      option.map(Success(_)).getOrElse(Failure(failure))

  }

}
