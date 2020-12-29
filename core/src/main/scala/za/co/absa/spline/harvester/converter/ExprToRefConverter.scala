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

package za.co.absa.spline.harvester.converter

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import za.co.absa.commons.lang.Converter
import za.co.absa.spline.harvester.ComponentCreatorFactory
import za.co.absa.spline.producer.model.v1_1.AttrOrExprRef

import scala.util.Try

// todo: use concrete converters instead of a factory? What about cyclic dependency on `expressionConverter`?
class ExprToRefConverter(converterFactory: ComponentCreatorFactory) extends Converter {
  override type From = SparkExpression
  override type To = AttrOrExprRef

  override def convert(arg: SparkExpression): AttrOrExprRef = {
    // todo: refactor it to partial functions
    Try {
      val attr = converterFactory.attributeConverter.convert(arg)
      AttrOrExprRef(Some(attr.id), None)
    }.recover {
      case _ =>
        val expr = converterFactory.expressionConverter.convert(arg)
        AttrOrExprRef(None, Some(expr.id))
    }.get
  }
}
