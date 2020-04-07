/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.harvester

import java.util.UUID

import org.apache.commons.configuration.{Configuration, SystemConfiguration}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.commons.io.TempFile
import za.co.absa.commons.json.DefaultJacksonJsonSerDe
import za.co.absa.commons.scalatest.ConditionalTestTags._
import za.co.absa.commons.version.Version._
import za.co.absa.spline.harvester.SparkLineageInitializer._
import za.co.absa.spline.harvester.SparkLineageInitializerSpec._
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer.ConfProperty._
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode._
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}
import za.co.absa.spline.test.fixture.{SparkFixture, SystemFixture}

class SparkLineageInitializerSpec
  extends AnyFunSpec
    with BeforeAndAfter
    with Matchers
    with MockitoSugar
    with SparkFixture.NewPerTest
    with SystemFixture.IsolatedSystemPropertiesPerTest {

  before {
    sys.props.put(LINEAGE_DISPATCHER_CLASS, classOf[MockLineageDispatcher].getName)
    MockLineageDispatcher.reset()
  }

  describe("codeless initialization") {

    it("should ignore subsequent programmatic init", ignoreIf(ver"$SPARK_VERSION" < ver"2.3")) {
      sys.props.put(SparkQueryExecutionListenersKey, classOf[SplineQueryExecutionListener].getName)
      withNewSparkSession(session => {
        session.enableLineageTracking()
        runDummySparkJob(session)
        MockLineageDispatcher.verifyTheOnlyLineageCaptured()
        MockLineageDispatcher.instanceCount should be(1)
      })
    }

    it("should propagate to child sessions", ignoreIf(ver"$SPARK_VERSION" < ver"2.3")) {
      sys.props.put(SparkQueryExecutionListenersKey, classOf[SplineQueryExecutionListener].getName)
      withNewSparkSession(session => {
        runDummySparkJob(session.newSession())
        MockLineageDispatcher.verifyTheOnlyLineageCaptured()
        MockLineageDispatcher.instanceCount should be(1)
      })
    }
  }

  describe("enableLineageTracking()") {
    it("should warn on double initialization", ignoreIf(ver"$SPARK_VERSION" < ver"2.3")) {
      withNewSparkSession(session => {
        session.enableLineageTracking() // 1st is fine
        MockLineageDispatcher.instanceCount should be(1)
        session.enableLineageTracking() // 2nd should warn
        MockLineageDispatcher.instanceCount should be(1)
      })
    }

    it("should return the spark session back to the caller") {
      withNewSparkSession(session =>
        session.enableLineageTracking() shouldBe session
      )
    }

    describe("modes") {
      it("should disable Spline and proceed, when is in BEST_EFFORT (default) mode") {
        sys.props += MODE -> BEST_EFFORT.toString

        withNewSparkSession(sparkSession => {
          sparkSession.enableLineageTracking(createFailingConfigurer())
          runDummySparkJob(sparkSession)
          MockLineageDispatcher.verifyNoLineageCaptured()
        })

        withNewSparkSession(sparkSession => {
          MockLineageDispatcher.onConstructionThrow(new SplineInitializationException("boom"))
          sparkSession.enableLineageTracking()
          runDummySparkJob(sparkSession)
          MockLineageDispatcher.verifyNoLineageCaptured()
        })
      }

      it("should abort application, when is in REQUIRED mode") {
        sys.props += MODE -> REQUIRED.toString

        intercept[Exception] {
          withNewSparkSession(_.enableLineageTracking(createFailingConfigurer()))
        }

        intercept[SplineInitializationException] {
          MockLineageDispatcher.onConstructionThrow(new SplineInitializationException("boom"))
          withNewSparkSession(_.enableLineageTracking())
        }
      }

      it("should have no effect, when is in DISABLED mode") {
        sys.props += MODE -> DISABLED.toString

        withNewSparkSession(sparkSession => {
          sparkSession.enableLineageTracking(createFailingConfigurer())
          runDummySparkJob(sparkSession)
          MockLineageDispatcher.verifyNoLineageCaptured()
        })

        withNewSparkSession(sparkSession => {
          sparkSession.enableLineageTracking()
          runDummySparkJob(sparkSession)
          MockLineageDispatcher.verifyNoLineageCaptured()
        })
      }
    }
  }
}

object SparkLineageInitializerSpec {

  class MockLineageDispatcher(conf: Configuration) extends LineageDispatcher {

    MockLineageDispatcher.onConstruction()

    override def send(plan: ExecutionPlan): String = MockLineageDispatcher.theMock.send(plan)

    override def send(event: ExecutionEvent): Unit = MockLineageDispatcher.theMock.send(event)
  }

  object MockLineageDispatcher extends MockitoSugar with DefaultJacksonJsonSerDe {
    private val theMock: LineageDispatcher = mock[LineageDispatcher]
    private[this] var throwableOnConstruction: Option[_ <: Throwable] = None
    private[this] var _instanceCount: Int = _

    private def onConstruction(): Unit = {
      this.throwableOnConstruction.foreach(throw _)
      this._instanceCount += 1
    }

    def instanceCount: Int = this._instanceCount

    def reset(): Unit = {
      this._instanceCount = 0
      Mockito.reset(theMock)
      when(theMock.send(any[ExecutionPlan]())) thenReturn UUID.randomUUID.toString.toJson
    }

    def onConstructionThrow(th: Throwable): Unit = {
      this.throwableOnConstruction = Some(th)
    }

    def verifyTheOnlyLineageCaptured(): Unit = {
      verify(theMock, times(1)).send(any[ExecutionPlan]())
      verify(theMock, times(1)).send(any[ExecutionEvent]())
    }

    def verifyNoLineageCaptured(): Unit = {
      verify(theMock, never()).send(any[ExecutionPlan]())
      verify(theMock, never()).send(any[ExecutionEvent]())
    }
  }

  private def runDummySparkJob(session: SparkSession): Unit = {
    import session.implicits._
    Seq((1, 2)).toDF.write.save(TempFile(pathOnly = true).deleteOnExit().path.toString)
  }

  private def createFailingConfigurer(): DefaultSplineConfigurer = new DefaultSplineConfigurer(new SystemConfiguration) {
    override lazy val lineageDispatcher: LineageDispatcher = sys.error("Testing exception - please ignore.")
  }

}
