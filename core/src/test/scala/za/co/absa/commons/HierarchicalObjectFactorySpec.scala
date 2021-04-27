package za.co.absa.commons

import org.apache.commons.configuration.{Configuration, MapConfiguration}
import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.HierarchicalObjectFactorySpec.{BarComponent, DummyComponenet, FooComponent}

class HierarchicalObjectFactorySpec extends AnyFlatSpec with Matchers {

  "createComponentsByKey()" should "create components" in {
    val rootObjFactory =
      new HierarchicalObjectFactory(
        new MapConfiguration(new java.util.HashMap[String, AnyRef] {
          put("test.child.names", "foo, bar")
          put("foo.className", classOf[FooComponent].getName)
          put("bar.className", classOf[BarComponent].getName)
        })
      ).child("test")

    val subDispatchers = rootObjFactory.createComponentsByKey[DummyComponenet]("child.names")

    subDispatchers should have length 2
    inside(subDispatchers) {
      case Seq(FooComponent(fooConf), BarComponent(barObjFactory)) =>
        fooConf.getString("className") should equal(classOf[FooComponent].getName)
        barObjFactory.configuration.getString("className") should equal(classOf[BarComponent].getName)
    }
  }

}

object HierarchicalObjectFactorySpec {

  trait DummyComponenet

  case class FooComponent(conf: Configuration) extends DummyComponenet

  case class BarComponent(objectFactory: HierarchicalObjectFactory) extends DummyComponenet

}
