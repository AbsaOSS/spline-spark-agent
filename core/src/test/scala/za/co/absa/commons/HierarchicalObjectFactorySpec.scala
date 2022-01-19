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

package za.co.absa.commons

import org.apache.commons.configuration.{Configuration, MapConfiguration}
import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.HierarchicalObjectFactorySpec.{BarComponent, DummyComponenet, Foo2Arg, Foo2Component, FooComponent}

class HierarchicalObjectFactorySpec extends AnyFlatSpec with Matchers {

  "createComponentsByKey()" should "create components" in {
    val rootObjFactory =
      new HierarchicalObjectFactory(
        new MapConfiguration(new java.util.HashMap[String, AnyRef] {
          put("test.child.names", "foo, foo2, bar")
          put("foo.className", classOf[FooComponent].getName)
          put("foo2.className", classOf[Foo2Component].getName)
          put("bar.className", classOf[BarComponent].getName)
        }),
        Seq(
          classOf[Foo2Arg] -> new Foo2Arg(42)
        )
      ).child("test")

    val subDispatchers = rootObjFactory.createComponentsByKey[DummyComponenet]("child.names")

    subDispatchers should have length 3
    inside(subDispatchers) {
      case Seq(FooComponent(fooConf), Foo2Component(foo2Conf, foo2Arg), BarComponent(barObjFactory)) =>
        fooConf.getString("className") should equal(classOf[FooComponent].getName)
        foo2Conf.getString("className") should equal(classOf[Foo2Component].getName)
        foo2Arg.x should equal(42)
        barObjFactory.configuration.getString("className") should equal(classOf[BarComponent].getName)
    }
  }

}

object HierarchicalObjectFactorySpec {

  trait DummyComponenet

  case class FooComponent(conf: Configuration) extends DummyComponenet

  class Foo2Arg(val x: Int)

  case class Foo2Component(conf: Configuration, foo2arg: Foo2Arg) extends DummyComponenet

  case class BarComponent(objectFactory: HierarchicalObjectFactory) extends DummyComponenet

}
