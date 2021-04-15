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

package za.co.absa.spline.harvester.json

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.harvester.json.HarvesterJsonSerDeSpec.Foo

class HarvesterJsonSerDeSpec
  extends AnyFlatSpec
    with Matchers {

  import HarvesterJsonSerDe.impl._

  behavior of "HarvesterJsonSerDe"

  it should "eliminate Nones, but preserve empty strings and empty collections" in {
    Foo(
      opt = None,
      str = "",
      seq = Seq.empty,
      map = Map.empty
    ).toJson.fromJson[Map[String, Any]] should equal(
      Map(
        // "opt" -> should be missing
        "str" -> "",
        "seq" -> Seq.empty,
        "map" -> Map.empty
      ))
  }

  it should "preserve nulls" in {
    Foo(
      opt = null,
      str = null,
      seq = null,
      map = null
    ).toJson.fromJson[Map[String, Any]] should equal(
      Map(
        "opt" -> null,
        "str" -> null,
        "seq" -> null,
        "map" -> null
      ))
  }
}

object HarvesterJsonSerDeSpec {

  case class Foo(
    str: String,
    opt: Option[Any],
    seq: Seq[Int],
    map: Map[String, Any]
  )

}
