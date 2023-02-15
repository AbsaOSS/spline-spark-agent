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

import org.apache.commons.lang3.StringUtils._
import org.json4s.{DefaultFormats, Formats, TypeHints}
import za.co.absa.commons.reflect.ReflectionUtils._
import za.co.absa.spline.model.dt.DataType

import java.text.SimpleDateFormat

object ShortTypeHintForSpline03ModelSupport {
  val formats: Formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = DefaultFormats.losslessDate.get

    override val typeHintFieldName: String = "_typeHint"

    override val typeHints: TypeHints = new TypeHints {
      private val classes: Seq[Class[_ <: DataType]] = directSubClassesOf[DataType]
      override val hints: List[Class[_]] = classes.toList

      override def hintFor(clazz: Class[_]): String = {
        val className = clazz.getName
        className.substring(1 + lastOrdinalIndexOf(className, ".", 2))
      }

      def classFor(hint: String): Option[Class[_]] = classes find (hintFor(_) == hint)
    }
  }
}
