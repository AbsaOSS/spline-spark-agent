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

import java.io._
import java.util.function.BiFunction
import java.util.{Collections, Properties, function}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.commons.lang.ImmutablePropertiesSpec._

import scala.collection.JavaConverters._
import scala.io.Source

object ImmutablePropertiesSpec {
  private val Props = ImmutableProperties(new Properties() {
    this.put("a", "1")
    this.put("b", "2")
  })
}

class ImmutablePropertiesSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  "apply()" should "deeply clone the source Properties instance" in {
    val mutableProps = new Properties() {
      this.put("a", "1")
      this.put("b", "2")
    }
    val immutableProps = ImmutableProperties(mutableProps)

    // try to mutate source properties
    mutableProps.put("b", "new value")
    mutableProps.put("c", "new entry")

    // immutable props should not be affected
    immutableProps should equal(Props)
  }

  "fromReader()" should "create a new instance from a reader" in {
    ImmutableProperties.fromReader(new StringReader(
      """
        |# a comment should be ignored
        |a = 1
        |b = 2
        |"""
        .stripMargin)) should equal(Props)
  }

  "fromStream()" should "create a new instance from a reader" in {
    ImmutableProperties.fromStream(new ByteArrayInputStream(
      """
        |# a comment should be ignored
        |a = 1
        |b = 2
        |"""
        .stripMargin.getBytes)) should equal(Props)
  }

  "empty()" should "return an empty instance" in {
    ImmutableProperties.empty should equal(ImmutableProperties(new Properties()))
  }

  "clone()" should "return itself" in {
    Props.clone() should be theSameInstanceAs Props
  }

  "equals()" should "return true if compared with another Properties impl" in {
    Props should equal(new Properties() {
      this.put("a", "1")
      this.put("b", "2")
    })
  }

  "equals()" should "return false not unequal instances" in {
    Props should not(equal(new Properties() {
      this.put("a", "1")
      this.put("b", "another value")
    }))

    Props should not(equal(new Properties() {
      this.put("a", "1")
      // missing entry 'b'
    }))

    Props should not(equal(new Properties() {
      this.put("a", "1")
      this.put("b", "2")
      this.put("c", "another entry")
    }))
  }

  "hashcode" should "meet the 'equals' contract" in {
    Props.hashCode should equal(new Properties() {
      this.put("a", "1")
      this.put("b", "2")
    }.hashCode)

    Props.hashCode should not(equal(new Properties() {
      this.put("a", "1")
      this.put("b", "another value")
    }.hashCode))

    Props.hashCode should not(equal(new Properties() {
      this.put("a", "1")
      // missing entry 'b'
    }.hashCode))

    Props.hashCode should not(equal(new Properties() {
      this.put("a", "1")
      this.put("b", "2")
      this.put("c", "another entry")
    }.hashCode))
  }

  "size()" should "return correct number of entries" in {
    Props.size should be(2)
  }

  "getProperty()" should "return correct value" in {
    Props.getProperty("a") should be("1")
    Props.getProperty("b") should be("2")
    Props.getProperty("b", "foo") should be("2")
    Props.getProperty("non-existing", "foo") should be("foo")
  }

  "propertyNames()" should "return set of keys" in {
    Props.propertyNames().asScala should have length 2
    Props.propertyNames().asScala.toSeq should contain theSameElementsAs Seq("a", "b")
  }

  "stringPropertyNames()" should "return set of keys" in {
    Props.stringPropertyNames().asScala should contain theSameElementsAs Set("a", "b")
  }

  "list()" should "list all properties" in {
    val writer = new StringWriter
    Props.list(new PrintWriter(writer))
    Source.fromString(writer.toString).getLines.toSeq should contain allElementsOf Seq("a=1", "b=2")
  }

  "toString()" should "list all properties" in {
    Props.toString should (include("a=1") and include("b=2"))
  }

  "store()" should "export all properties" in {
    val writer = new StringWriter
    Props.store(writer, "foo")
    val result = Source.fromString(writer.toString).getLines.patch(1, Iterator.empty, 1).map(_.trim).toSeq
    result should contain theSameElementsAs Seq("#foo", "b=2", "a=1")
  }

  "All mutable methods" should "be prohibited" in {
    intercept[UnsupportedOperationException](Props.put("x", "y"))
    intercept[UnsupportedOperationException](Props.putIfAbsent("x", "y"))
    intercept[UnsupportedOperationException](Props.putAll(Collections.emptyMap()))
    intercept[UnsupportedOperationException](Props.compute("x", mock[BiFunction[AnyRef, AnyRef, AnyRef]]))
    intercept[UnsupportedOperationException](Props.computeIfAbsent("x", mock[function.Function[AnyRef, AnyRef]]))
    intercept[UnsupportedOperationException](Props.computeIfPresent("x", mock[BiFunction[AnyRef, AnyRef, AnyRef]]))
    intercept[UnsupportedOperationException](Props.clear())
    intercept[UnsupportedOperationException](Props.load(mock[Reader]))
    intercept[UnsupportedOperationException](Props.load(mock[InputStream]))
    intercept[UnsupportedOperationException](Props.loadFromXML(mock[InputStream]))
    intercept[UnsupportedOperationException](Props.merge("x", "y", mock[BiFunction[AnyRef, AnyRef, AnyRef]]))
    intercept[UnsupportedOperationException](Props.rehash())
    intercept[UnsupportedOperationException](Props.remove("x"))
    intercept[UnsupportedOperationException](Props.remove("x", "y"))
    intercept[UnsupportedOperationException](Props.replace("x", "y"))
    intercept[UnsupportedOperationException](Props.replace("x", "y", "z"))
    intercept[UnsupportedOperationException](Props.replaceAll(mock[BiFunction[AnyRef, AnyRef, AnyRef]]))
  }
}
