/*
 * Copyright 2023 ABSA Group Limited
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

package za.co.absa.spline.commons.reflect

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.commons.reflect.ReflectionUtils.ModuleClassSymbolExtractor
import za.co.absa.spline.commons.reflect.ReflectionUtilsSpec._

import scala.reflect.ClassTag

class ReflectionUtilsSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  behavior of "ReflectionUtils"

  behavior of "extractProperties()"

  it should "for given Product return a map of element names to their values" in {
    // case class
    ReflectionUtils.extractProperties(Foo("aaa")) should be(Map("x" -> "aaa", "y" -> 42))
    ReflectionUtils.extractProperties(Foo("aaa", 777)) should be(Map("x" -> "aaa", "y" -> 777))
    // normal class
    ReflectionUtils.extractProperties(new Bar("bbb", 42)) should be(Map("a" -> "bbb"))
  }

  behavior of "extractValue()"

  it should "return a value of a private field of a Scala object" in {
    ReflectionUtils.extractValue[Int](Foo, "privateVal") should be(42)
  }

  it should "return a value of a private field of a Java object" in {
    ReflectionUtils.extractValue[Array[Char]]("foo", "value") should equal("foo".toCharArray)
  }

  it should "return values of compiler generated private fields" in {
    val bar = new Bar("Pi", 3.14)
    ReflectionUtils.extractValue[String](bar, "a") shouldEqual "Pi"
    ReflectionUtils.extractValue[Double](bar, "b") shouldEqual 3.14
  }

  it should "extract from a field declared in any of the superclasses" in {
    class SubBar(a: String, b: Double) extends Bar(a, b)
    val subSubBar = new SubBar("Pi", 3.14) {}
    ReflectionUtils.extractValue[String](subSubBar, "a") shouldEqual "Pi"
    ReflectionUtils.extractValue[Double](subSubBar, "b") shouldEqual 3.14
  }

  it should "extract from a field using a provided class tag" in {
    val subBar = new Bar("Pi", 3.14) {}
    ReflectionUtils.extractValue[Bar, String](subBar, "a") shouldEqual "Pi"
  }

  it should "extract from a private field used in a lambda" in {
    class A(x: Int) { () => x
    }
    ReflectionUtils.extractValue[Int](new A(42), "x") should equal(42)
    ReflectionUtils.extractValue[A, Int](new A(42), "x") should equal(42)
  }

  it should "extract from a private field declared in a trait" in {
    ReflectionUtils.extractValue[Boolean](MyObject, "z") should be(true)
    ReflectionUtils.extractValue[Boolean](new MyClass, "z") should be(true)
    ReflectionUtils.extractValue[MyClass, Boolean](new MyClass, "z") should be(true)
  }

  //noinspection ScalaUnusedSymbol
  it should "extract from a lazy val of inner classes" in {
    trait T {
      private lazy val z: Int = 42
    }
    object O extends T {
      private lazy val zz: Seq[Nothing] = Seq.empty
    }
    ReflectionUtils.extractValue[Int](O, "z") should equal(42)
    ReflectionUtils.extractValue[Seq[_]](O, "zz") should equal(Seq.empty)
    ReflectionUtils.extractValue[Int](O, "bitmap$0") should equal(0x3)
  }

  it should "extract from a lazy val of outer classes" in {
    ReflectionUtils.extractValue[Boolean](Lazy, "z") should be(42)
    ReflectionUtils.extractValue[Boolean](Lazy, "bitmap$0") should be(true)
  }

  it should "not confuse accessors with methods" in {
    object AAA {
      private def x[A: ClassTag]: Int = sys.error("don't call me")
    }
    intercept[NoSuchFieldException] {
      ReflectionUtils.extractValue[Int](AAA, "x")
    }
  }

  it should "call accessors if available" in {
    object AAA {
      private def x[A: ClassTag]: Int = sys.error("don't call me")

      private def x: Int = 42
    }
    ReflectionUtils.extractValue[Int](AAA, "x") should be(42)
  }

  // A workaround for https://github.com/scala/bug/issues/12190
  it should "fallback to Java reflection when Scala one fails" in {
    @CyclicAnnotationExample.CyclicAnnotation
    object Foo {
      val x = 42

      def y = 666

      def z() = 123
    }
    ReflectionUtils.extractValue[Int](Foo, "x") should be(Foo.x)
    ReflectionUtils.extractValue[Int](Foo, "y") should be(Foo.y)
    ReflectionUtils.extractValue[Int](Foo, "z") should be(Foo.z)
  }

  it should "fallback to Java reflection when Scala one fails via return type" in {
    val innerObj = new ClassWithCyclicAnnotation
    val obj = WrappingClass(innerObj)
    ReflectionUtils.extractValue[ClassWithCyclicAnnotation](obj, "x") should be(innerObj)
  }

  it should "extract field from java class" in {
    val obj = new JavaClassExample(42)
    ReflectionUtils.extractValue[ClassWithCyclicAnnotation](obj, "value") should be(42)
  }

  it should "call method with zero parameters in java class" in {
    val obj = new JavaClassExample(42)
    ReflectionUtils.extractValue[ClassWithCyclicAnnotation](obj, "getValuePlusOne") should be(43)
  }

  behavior of "ModuleClassSymbolExtractor"

  it should "extract objects" in {
    ModuleClassSymbolExtractor.unapply(Foo) should be(defined)
    ModuleClassSymbolExtractor.unapply("Bar") should not be defined
    ModuleClassSymbolExtractor.unapply({}) should not be defined
    ModuleClassSymbolExtractor.unapply(Array()) should not be defined
  }

  it should "not blow up on nulls or instances of synthetic classes" in {
    ModuleClassSymbolExtractor.unapply(null)
    ModuleClassSymbolExtractor.unapply((x: Int) => x + 1)
  }

  behavior of "allInterfacesOf()"

  it should "return all interfaces that the given class implements included inherited ones" in {
    trait T1
    trait T2 extends T1
    trait T3 extends T2
    trait T4 extends T3 with T1

    trait TX
    trait TY
    trait TZ

    abstract class A extends T4 with TX
    class B extends A with T1 with TY with TZ

    val expectedSet = Set(
      classOf[T1],
      classOf[T2],
      classOf[T3],
      classOf[T4],
      classOf[TX],
      classOf[TY],
      classOf[TZ]
    )

    ReflectionUtils.allInterfacesOf[B] should equal(expectedSet)
    ReflectionUtils.allInterfacesOf(classOf[B]) should equal(expectedSet)
  }
}

object ReflectionUtilsSpec {

  sealed trait MyTrait extends Serializable {
    private val z: Boolean = true
    assert(z)
  }

  class MyClass extends MyTrait with Serializable

  object MyObject extends MyTrait

  case class Foo(x: String, y: Int = 42)

  object Foo {
    //noinspection ScalaUnusedSymbol
    private[this] val privateVal = 42
  }

  class Bar(val a: String, b: Double) {
    def methodUsingFields: String = a + b.toString
  }

  object Lazy {
    lazy val z = 42
  }

  case class WrappingClass(x: ClassWithCyclicAnnotation)
}
