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

package za.co.absa.spline.harvester.postprocessing.extra.model.template

import org.apache.spark.internal.Logging

import javax.script.ScriptEngine
import scala.util.{Failure, Success, Try}

class ExtraTemplate(extra: Map[String, Any]) extends Logging {

  def eval(bindings: Map[String, Any]): Map[String, Any] =
    Try(tryEval(bindings)) match {
      case Success(value) => value
      case Failure(e) =>
        logWarning("DeclarativeExtraInjectingFilter's template evaluation failed, filter will not be applied.", e)
        Map.empty
    }

  private def tryEval(bindings: Map[String, Any]): Map[String, Any] =
    extra.transform((k, v) => evalValue(v, bindings))

  private def evalValue(value: Any, bindings: Map[String, Any]): Any = value match {
    case m: Map[String, _] => m.transform((k, v) => evalValue(v, bindings))
    case s: Seq[_] => s.map(evalValue(_, bindings))
    case e: Evaluable => e.eval(bindings)
    case v => v
  }
}

sealed trait Evaluable {
  def eval(bindings: Map[String, Any]): AnyRef
}

case class JVMProp(propName: String) extends Evaluable {
  override def eval(bindings: Map[String, Any]): AnyRef = System.getProperty(propName)
}

case class EnvVar(envName: String) extends Evaluable {
  override def eval(bindings: Map[String, Any]): AnyRef = System.getenv(envName)
}

case class JsEval(jsEngine: ScriptEngine, js: String) extends Evaluable {
  override def eval(bindings: Map[String, Any]): AnyRef = {

    val jsBindings = jsEngine.createBindings
    bindings.foreach { case (k, v) => jsBindings.put(k, v) }

    jsEngine.eval(js, jsBindings)
  }
}

object EvaluableNames {
  val JVMProp = "$jvm"
  val EnvVar = "$env"
  val JsEval = "$js"
}
