/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.spline.harvester.postprocessing.metadata

import javax.script.ScriptEngineManager
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsEvalSpec extends AnyFlatSpec with Matchers {

  behavior of "JsEval.eval()"

  it should "unwrap a JavaScript array into a Seq" in {
    val engine = new ScriptEngineManager().getEngineByMimeType("text/javascript")
    // there is no JS engine on JDK 15+ as Nashorn was removed from it
    assume(engine != null, "no JavaScript engine available on this JVM")

    JsEval(engine, "[1, 2, 3]").eval(Map.empty) shouldBe Seq(1, 2, 3)
  }
}
