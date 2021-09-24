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

package za.co.absa.spline.harvester.postprocessing.extra


import za.co.absa.spline.harvester.postprocessing.extra.ExtraTemplateModel._

import javax.script.ScriptEngineManager

object ExtraTemplateParser {

  private val jsEngine = new ScriptEngineManager().getEngineByMimeType("text/javascript")

  def parse(extra: Map[String, Any]): ExtraTemplate = {
    new ExtraTemplate(extra.transform((k, v) => parseRec(v)))
  }

  private def parseRec(v: Any): Any = v match {
    case m: Map[String,_] if m.contains(EvaluableNames.JVMProp) =>
      JVMProp(m(EvaluableNames.JVMProp).asInstanceOf[String])

    case m: Map[String,_] if m.contains(EvaluableNames.EnvVar) =>
      EnvVar(m(EvaluableNames.EnvVar).asInstanceOf[String])

    case m: Map[String,_] if m.contains(EvaluableNames.JsEval) =>
      JsEval(jsEngine, m(EvaluableNames.JsEval).asInstanceOf[String])

    case m: Map[String,_] => m.transform((k, v) => parseRec(v))
    case s: Seq[_] => s.map(parseRec)
    case v => v
  }
}
