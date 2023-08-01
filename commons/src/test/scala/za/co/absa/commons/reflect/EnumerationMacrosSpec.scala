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

package za.co.absa.commons.reflect

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.reflect.EnumerationMacrosSpec.Color.{Blue, Green, Red}
import za.co.absa.commons.reflect.EnumerationMacrosSpec._

class EnumerationMacrosSpec extends AnyFlatSpec with Matchers {

  "sealedInstancesOf[]" should "return all direct instances of a given sealed trait" in {
    Color.values shouldEqual Set(Red, Green, Blue)
  }

}

object EnumerationMacrosSpec {

  sealed trait Color

  object Color {
    val values: Set[Color] = EnumerationMacros.sealedInstancesOf[Color]

    case object Red extends Color

    case object Green extends Color

    case object Blue extends Color

  }


}
