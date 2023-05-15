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

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.util.{DefaultIndenter, DefaultPrettyPrinter}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object HarvesterJsonSerDe {

  object impl {
    private val mapper = {
      new ObjectMapper()
        .registerModule(DefaultScalaModule)
        .setSerializationInclusion(Include.NON_ABSENT)
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
