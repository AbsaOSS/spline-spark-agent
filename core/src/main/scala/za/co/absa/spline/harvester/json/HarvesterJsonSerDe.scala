/*
 * Copyright 2019 ABSA Group Limited
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

import org.json4s.Extraction.decompose
import org.json4s.ext.JavaTypesSerializers
import org.json4s.native.JsonMethods.{compact, parse, pretty, render}
import org.json4s.{Formats, StringInput}

object HarvesterJsonSerDe {

  object impl {
    private implicit val formats: Formats =
      ShortTypeHintForSpline03ModelSupport.formats ++
        JavaTypesSerializers.all

    implicit class EntityToJson[B <: AnyRef](entity: B) {
      def toJson: String = compact(render(decompose(entity)(formats)))
    }

    implicit class JsonToEntity(json: String) {
      def fromJson[B: Manifest]: B = parseJson().extract(formats, implicitly[Manifest[B]])

      def asPrettyJson: String = pretty(render(parseJson()))

      private def parseJson() = parse(StringInput(json), formats.wantsBigDecimal, formats.wantsBigInt)
    }
  }
}
