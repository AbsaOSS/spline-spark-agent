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

package za.co.absa.commons.lang

import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.commons.lang.TypeNegationConstraintSpec.LunchFixture

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.tools.reflect.{ToolBox, ToolBoxError}

class TypeNegationConstraintSpec extends AnyFlatSpec with LunchFixture {
  it should "compile" in {
    toolBox compile q"$lunch.feedVegetarian($food.Apple)" // cool
    toolBox compile q"$lunch.feedVegetarian($food.Salmon)" // that's good too
  }

  it should "not compile" in intercept[ToolBoxError] {
    toolBox compile q"$lunch.feedVegetarian($food.Pork)" // No meat please!
  }
}

object TypeNegationConstraintSpec {

  trait LunchFixture {
    val toolBox: ToolBox[universe.type] = runtimeMirror(getClass.getClassLoader).mkToolBox()

    val food: toolBox.u.Symbol = toolBox.define(
      q"""
      object Food {
        trait Food

        trait Meat extends Food
        trait Fish extends Food
        trait Fruit extends Food

        object Pork extends Meat
        object Salmon extends Fish
        object Apple extends Fruit
      }
      """)

    val lunch: toolBox.u.Symbol = toolBox.define(
      q"""
      object Lunch {
        import za.co.absa.commons.lang.TypeConstraints._
        import $food._
        def feedVegetarian[A <: Food : not[Meat]#λ](food: A): Unit = {}
      }
      """)

  }

}
