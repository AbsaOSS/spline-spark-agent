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

import scala.concurrent.{ExecutionContext, Future}

object FutureImplicits {

  implicit class FutureWrapper[T](val future: Future[T]) extends AnyVal {

    /**
      * Executes the body after the future is completed (no matter if successfully or not)
      * Returns the future with the same value/exception
      *
      * Analogous to finally block in try-catch-finally statement
      * Unlike onComplete this guarantees that body will be executed before any other operations on the future
      *
      * @param body function executed for a side effect
      * @return unchanged future that it was called on
      */
    def finallyDo(body: => Unit)(implicit executor: ExecutionContext): Future[T] = {
      future.transform(
        value => {
          body
          value
        },
        exception => {
          body
          exception
        }
      )
    }
  }
}
