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

import za.co.absa.commons.version.impl.SemVer20Impl.SemanticVersion
import za.co.absa.commons.version.impl.{SemVer20Impl, SimpleVersionImpl}

import scala.annotation.tailrec

case class Version(components: Component*) extends Ordered[Version] {
  override def compare(that: Version): Int = components
    .zipAll(that.components, EmptyComponent, EmptyComponent)
    .map({ case (xi, yi) => xi compare yi })
    .find(0.!=)
    .getOrElse(0)
}

object Version extends SimpleVersionImpl with SemVer20Impl {

  implicit class VersionStringInterpolator(val sc: StringContext) extends AnyVal {

    def ver(args: Any*): Version = Version.asSimple(sc.s(args: _*))

    def semver(args: Any*): SemanticVersion = Version.asSemVer(sc.s(args: _*))
  }

  implicit class VersionExtensionMethods(val v: Version) extends AnyVal {

    def asString: String = {
      @tailrec def loop(sb: StringBuilder, delim: String, comps: List[Component]): StringBuilder = comps match {
        case Nil => sb
        case NumericComponent(x) :: cs => loop(sb.append(delim).append(x), ".", cs)
        case StringComponent(s) :: cs => loop(sb.append(delim).append(s), ".", cs)
        case PreReleaseComponent(identifiers@_*) :: cs => loop(sb, "-", identifiers.toList ++ cs)
        case BuildMetadataComponent(identifiers@_*) :: cs => loop(sb, "+", identifiers.toList ++ cs)
      }

      loop(StringBuilder.newBuilder, "", v.components.toList).result()
    }
  }

}
