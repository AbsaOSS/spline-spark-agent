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

package za.co.absa.commons.version

/**
  * Represents one section of a version string
  * E.g. for the version string "1.foo.42" the components would be "1", "foo" and "42".
  */
sealed trait Component extends Ordered[Component] {
  final override def compare(that: Component): Int = {
    val c = comparator
    if (c isDefinedAt that) c(that)
    else -(that compare this)
  }

  def comparator: PartialFunction[Component, Int]
}

object Component {
  def apply(s: String): Component =
    if (s.forall(_.isDigit)) NumericComponent(BigInt(s))
    else StringComponent(s)
}

/**
  * An empty component.
  * It's less than any other component, unless the other component explicitly states otherwise.
  */
case object EmptyComponent extends Component {
  override def comparator: PartialFunction[Component, Int] = {
    case EmptyComponent => 0
  }
}

/**
  * A numeric component.
  * Compares naturally to itself.
  */
case class NumericComponent(x: BigInt) extends Component {
  override def comparator: PartialFunction[Component, Int] = {
    case EmptyComponent => +1
    case NumericComponent(y) => x compare y
  }
}

/**
  * A string component.
  * String components are compared lexicographically and have higher precedence than numeric components.
  */
case class StringComponent(s: String) extends Component {
  override def comparator: PartialFunction[Component, Int] = {
    case EmptyComponent => +1
    case NumericComponent(_) => +1
    case StringComponent(t) => s compare t
  }
}

/**
  * A pre-release component [SemVer]
  *
  * Precedence for a pre-release component is determined by comparing each identifier from left to right
  * until a difference is found as follows:
  * - identifiers consisting of only digits are compared numerically
  * - identifiers with letters or hyphens are compared lexically in ASCII sort order
  * - numeric identifiers always have lower precedence than non-numeric identifiers
  * - a larger set of fields has a higher precedence than a smaller set, if all of the preceding identifiers are equal
  *
  * Example: alpha < alpha.1 < alpha.beta < beta < beta.2 < beta.11 < rc.1
  *
  * A pre-release component have a lower precedence than an empty component.
  *
  * @see https://semver.org/spec/v2.0.0.html#spec-item-9
  * @param identifiers build identifiers
  */
case class PreReleaseComponent(identifiers: Component*) extends Component {
  require(identifiers.nonEmpty)

  override def comparator: PartialFunction[Component, Int] = {
    case EmptyComponent => -1
    case NumericComponent(_) => -1
    case StringComponent(_) => -1
    case that: PreReleaseComponent =>
      Version(this.identifiers: _*) compare Version(that.identifiers: _*)
  }
}

/**
  * A build metadata component [SemVer]
  * It behaves as {{EmptyComponent}} when comparing to other components, i.e. it doesn't count.
  *
  * @see https://semver.org/spec/v2.0.0.html#spec-item-10
  * @param identifiers build identifiers
  */
case class BuildMetadataComponent(identifiers: Component*) extends Component {
  require(identifiers.nonEmpty)

  override def comparator: PartialFunction[Component, Int] = {
    case other: Component => EmptyComponent compare other
  }
}
