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

package za.co.absa.commons.lang

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.lang.CachingConverterSpec.{CountingConverter, StrToIntConverter}

class CachingConverterSpec extends AnyFlatSpec with Matchers {

  behavior of "CachingConverter"

  it should "be memoized" in {
    object TestingConverter extends StrToIntConverter with CountingConverter with CachingConverter

    TestingConverter.calls should be(0)
    TestingConverter("42") should equal(42)
    TestingConverter.calls should be(1)
    TestingConverter("42") should equal(42)
    TestingConverter.calls should be(1)
    TestingConverter("77") should equal(77)
    TestingConverter.calls should be(2)
  }

  it should "support custom keys" in {
    object TestingConverter extends StrToIntConverter with CountingConverter with CachingConverter {
      override protected def keyOf(x: String): Key = x match {
        case "zero" => "0"
        case _ => super.keyOf(x)
      }
    }

    TestingConverter.calls should be(0)
    TestingConverter("0") should equal(0)
    TestingConverter.calls should be(1)
    TestingConverter("zero") should equal(0)
    TestingConverter.calls should be(1)
    TestingConverter("42") should equal(42)
    TestingConverter.calls should be(2)
  }
}

object CachingConverterSpec {

  trait StrToIntConverter extends Converter {
    override type From = String
    override type To = Int
    override def convert(arg: String): Int = arg.toInt
  }

  trait CountingConverter extends Converter {
    private var _calls = 0
    def calls: Int = _calls

    abstract override def convert(arg: From): To = {
      _calls += 1
      super.convert(arg)
    }
  }

}
