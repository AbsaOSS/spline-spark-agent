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


import za.co.absa.shaded.jackson.annotation.JsonInclude
import za.co.absa.shaded.jackson.core.JsonGenerator
import za.co.absa.shaded.jackson.core.util.{DefaultIndenter, DefaultPrettyPrinter}
import za.co.absa.shaded.jackson.databind.{ObjectMapper, SerializationFeature}
import za.co.absa.shaded.jackson.module.scala.DefaultScalaModule

object HarvesterJsonSerDe {

  object impl {
    private val mapper = {
      val m = new ObjectMapper()
        .registerModule(DefaultScalaModule)
        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

      m.configOverride(classOf[scala.Option[_]]).setInclude(
        JsonInclude.Value.empty
          .withValueInclusion(JsonInclude.Include.CUSTOM)
          .withValueFilter(classOf[SplineOptionFilter]))

      m
    }

    private class SplineOptionFilter {
      override def equals(o: Any): Boolean = o match {
        case None => true
        case _ => false
      }
    }

    private lazy val prettyWriter = {
      class CustomPrettyPrinter extends DefaultPrettyPrinter {
        this.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)

        override def writeObjectFieldValueSeparator(jg: JsonGenerator): Unit = jg.writeRaw(": ")

        override def createInstance = new CustomPrettyPrinter
      }
      mapper.writer(new CustomPrettyPrinter)
    }


    implicit class EntityToJson[B <: AnyRef](entity: B) {
      def toJson: String = {
        mapper.writeValueAsString(entity)
      }
    }

    implicit class JsonToEntity(json: String) {
      def fromJson[B: Manifest]: B = {
        mapper.readValue[B](
          json,
          implicitly[Manifest[B]].runtimeClass.asInstanceOf[Class[B]]
        )
      }

      def asPrettyJson: String = {
        val jsonObject = mapper.readValue(json, classOf[AnyRef])
        val prettyJson = prettyWriter.writeValueAsString(jsonObject)
        prettyJson
      }
    }
  }
}
