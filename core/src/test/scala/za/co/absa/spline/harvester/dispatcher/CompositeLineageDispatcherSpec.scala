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

package za.co.absa.spline.harvester.dispatcher

import org.apache.commons.configuration.{Configuration, MapConfiguration}
import org.mockito.Mockito._
import org.scalatest.Inside.inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.harvester.conf.HierarchicalObjectFactory
import za.co.absa.spline.harvester.dispatcher.CompositeLineageDispatcherSpec.{BarDispatcher, FooDispatcher}
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

import java.util

class CompositeLineageDispatcherSpec
  extends AnyFlatSpec
    with MockitoSugar
    with Matchers {

  it should "delegate calls" in {
    val dummyExecPlan = mock[ExecutionPlan]
    val dummyExecEven = mock[ExecutionEvent]
    val mockDispatcher1 = mock[LineageDispatcher]
    val mockDispatcher2 = mock[LineageDispatcher]

    val compositeDispatcher = new CompositeLineageDispatcher(mockDispatcher1, mockDispatcher2)
    compositeDispatcher.send(dummyExecPlan)
    compositeDispatcher.send(dummyExecEven)

    verify(mockDispatcher1).send(dummyExecPlan)
    verify(mockDispatcher2).send(dummyExecPlan)

    verify(mockDispatcher1).send(dummyExecEven)
    verify(mockDispatcher2).send(dummyExecEven)

    verifyNoMoreInteractions(mockDispatcher1)
    verifyNoMoreInteractions(mockDispatcher2)
  }

  it should "not break on empty delegatees" in {
    val compositeDispatcher = new CompositeLineageDispatcher()

    compositeDispatcher.send(mock[ExecutionPlan])
    compositeDispatcher.send(mock[ExecutionEvent])

    succeed
  }

  "getDispatchers()" should "create sub-dispatchers" in {
    val rootObjFactory = new HierarchicalObjectFactory(new MapConfiguration(new util.HashMap[String, AnyRef] {
      put("test.dispatchers", "foo, bar")
      put("foo.className", classOf[FooDispatcher].getName)
      put("bar.className", classOf[BarDispatcher].getName)
    }))

    val subDispatchers = CompositeLineageDispatcher.getDispatchers(rootObjFactory.child("test"))

    subDispatchers should have length 2
    inside(subDispatchers) {
      case Seq(FooDispatcher(fooConf), BarDispatcher(barObjFactory)) =>
        fooConf.getString("className") should equal(classOf[FooDispatcher].getName)
        barObjFactory.configuration.getString("className") should equal(classOf[BarDispatcher].getName)
    }
  }
}

object CompositeLineageDispatcherSpec {

  trait DummyLineageDispatcher extends LineageDispatcher {
    override def send(executionPlan: ExecutionPlan): Unit = ()
    override def send(event: ExecutionEvent): Unit = ()
  }

  case class FooDispatcher(conf: Configuration) extends DummyLineageDispatcher

  case class BarDispatcher(objectFactory: HierarchicalObjectFactory) extends DummyLineageDispatcher

}
