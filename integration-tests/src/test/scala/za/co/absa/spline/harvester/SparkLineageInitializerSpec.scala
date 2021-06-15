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

import org.apache.commons.configuration.{Configuration, SystemConfiguration}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfter, Succeeded}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.mockito.MockitoSugar.mock
import za.co.absa.commons.io.TempFile
import za.co.absa.commons.json.DefaultJacksonJsonSerDe
import za.co.absa.commons.scalatest.ConditionalTestTags._
import za.co.absa.commons.version.Version._
import za.co.absa.spline.harvester.SparkLineageInitializer._
import za.co.absa.spline.harvester.SparkLineageInitializerSpec.{MockLineageDispatcher, _}
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer.ConfProperty._
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode._
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}
import za.co.absa.spline.test.fixture.{SparkFixture, SystemFixture}

import scala.concurrent.{Future, Promise}

class SparkLineageInitializerSpec
  extends AsyncFlatSpec
    with BeforeAndAfter
    with Matchers
    with MockitoSugar
    with SparkFixture.NewPerTest
    with SystemFixture.IsolatedSystemPropertiesPerTest {

  before {
    sys.props.put(RootLineageDispatcher, "test")
    sys.props.put(s"$RootLineageDispatcher.test.className", classOf[MockLineageDispatcher].getName)
    MockLineageDispatcher.reset()
  }

  behavior of "codeless initialization"

  it should "ignore subsequent programmatic init" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    sys.props.put(SparkQueryExecutionListenersKey, classOf[SplineQueryExecutionListener].getName)
    withSparkSession { session =>
      session.enableLineageTracking()
      val future = onSuccessListenerFuture(session)

      runDummySparkJob(session)

      future.map { _ =>
        MockLineageDispatcher.verifyTheOnlyLineageCaptured()
        MockLineageDispatcher.instanceCount should be(1)
      }
    }
  }

  it should "propagate to child sessions" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    sys.props.put(SparkQueryExecutionListenersKey, classOf[SplineQueryExecutionListener].getName)
    withSparkSession { session =>
      val subSession = session.newSession()

      val future = onSuccessListenerFuture(subSession)

      runDummySparkJob(subSession)

      future.map { _ =>
        MockLineageDispatcher.verifyTheOnlyLineageCaptured()
      }
    }
  }

  behavior of "enableLineageTracking()"

  it should "warn on double initialization" taggedAs ignoreIf(ver"$SPARK_VERSION" < ver"2.3") in {
    withSparkSession { session =>
      session.enableLineageTracking() // 1st is fine
      MockLineageDispatcher.instanceCount should be(1)
      session.enableLineageTracking() // 2nd should warn
      MockLineageDispatcher.instanceCount should be(1)
    }
  }

  it should "allow user to start again after error" in {
    sys.props += Mode -> BEST_EFFORT.toString

    withSparkSession { sparkSession =>
      sparkSession.enableLineageTracking(createFailingConfigurer())
      runDummySparkJob(sparkSession)

      // second attempt
      sparkSession.enableLineageTracking()
      val future = onSuccessListenerFuture(sparkSession)

      runDummySparkJob(sparkSession)

      future.map { _ =>
        MockLineageDispatcher.verifyTheOnlyLineageCaptured()
      }
    }
  }

  it should "return the spark session back to the caller" in {
    withSparkSession(session =>
      session.enableLineageTracking() shouldBe session
    )
  }

  behavior of "modes"

  it should "disable Spline and proceed, when is in BEST_EFFORT mode after configurer fail" in {
    sys.props += Mode -> BEST_EFFORT.toString

    withSparkSession { sparkSession =>
      sparkSession.enableLineageTracking(createFailingConfigurer())
      val future = onSuccessListenerFuture(sparkSession)

      runDummySparkJob(sparkSession)

      future.map { _ =>
        MockLineageDispatcher.verifyNoLineageCaptured()
      }
    }
  }

  it should "disable Spline and proceed, when is in BEST_EFFORT mode after exception" in {
    sys.props += Mode -> BEST_EFFORT.toString

    withNewSparkSession { sparkSession =>
      MockLineageDispatcher.onConstructionThrow(new SplineInitializationException("boom"))
      sparkSession.enableLineageTracking()
      val future = onSuccessListenerFuture(sparkSession)

      runDummySparkJob(sparkSession)

      future.map { _ =>
        MockLineageDispatcher.verifyNoLineageCaptured()
      }
    }
  }

  it should "abort application, when is in REQUIRED mode" in {
    sys.props += Mode -> REQUIRED.toString

    intercept[Exception] {
      withSparkSession(_.enableLineageTracking(createFailingConfigurer()))
    }

    intercept[SplineInitializationException] {
      MockLineageDispatcher.onConstructionThrow(new SplineInitializationException("boom"))
      withNewSparkSession(_.enableLineageTracking())
    }

    Succeeded
  }

  it should "have no effect, when is in DISABLED mode after configurer fail" in {
    sys.props += Mode -> DISABLED.toString

    withSparkSession { sparkSession =>
      sparkSession.enableLineageTracking(createFailingConfigurer())
      val future = onSuccessListenerFuture(sparkSession)

      runDummySparkJob(sparkSession)

      future.map { _ =>
        MockLineageDispatcher.verifyNoLineageCaptured()
      }
    }
  }

  it should "have no effect, when is in DISABLED mode" in {
    sys.props += Mode -> DISABLED.toString

    withNewSparkSession { sparkSession =>
      sparkSession.enableLineageTracking()
      val future = onSuccessListenerFuture(sparkSession)

      runDummySparkJob(sparkSession)

      future.map { _ =>
        MockLineageDispatcher.verifyNoLineageCaptured()
      }
    }
  }

}

object SparkLineageInitializerSpec {

  class MockLineageDispatcher(conf: Configuration) extends LineageDispatcher {

    MockLineageDispatcher.onConstruction()

    override def send(plan: ExecutionPlan): Unit = MockLineageDispatcher.theMock.send(plan)

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
      this.throwableOnConstruction = None
      Mockito.reset(theMock)
    }

    def onConstructionThrow(th: Throwable): Unit = {
      this.throwableOnConstruction = Some(th)
    }

    def verifyTheOnlyLineageCaptured(): Assertion = {
      verify(theMock, times(1)).send(any[ExecutionPlan]())
      verify(theMock, times(1)).send(any[ExecutionEvent]())
      Succeeded
    }

    def verifyNoLineageCaptured(): Assertion = {
      verify(theMock, never()).send(any[ExecutionPlan]())
      verify(theMock, never()).send(any[ExecutionEvent]())
      Succeeded
    }
  }

  private def runDummySparkJob(session: SparkSession): Unit = {
    import session.implicits._
    Seq((1, 2)).toDF.write.save(TempFile(pathOnly = true).deleteOnExit().path.toString)
  }

  private def createFailingConfigurer(): DefaultSplineConfigurer =
    new DefaultSplineConfigurer(mock[SparkSession], new SystemConfiguration) {
      override def lineageDispatcher: LineageDispatcher = sys.error("Testing exception - please ignore.")
    }

  private def onSuccessListenerFuture(spark: SparkSession): Future[Unit] = {
    val promise = Promise[Unit]

    spark.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = promise.success()

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = () // NoOp
    })

    promise.future
  }

}
