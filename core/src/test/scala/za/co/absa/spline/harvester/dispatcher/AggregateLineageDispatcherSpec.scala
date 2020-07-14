package za.co.absa.spline.harvester.dispatcher

import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.producer.model.ExecutionPlan

class AggregateLineageDispatcherSpec
  extends AnyFlatSpec
  with MockitoSugar
  with Matchers {

  it should "dispatch execution plans to all dispatchers but only return the primary dispatcher return value" in {
    val executionPlan: ExecutionPlan = mock[ExecutionPlan]
    val dispatcherA: LineageDispatcher = mock[LineageDispatcher]
    val dispatcherB: LineageDispatcher = mock[LineageDispatcher]
    val dispatcherC: LineageDispatcher = mock[LineageDispatcher]

    when(dispatcherA.send(executionPlan)).thenReturn("a")
    when(dispatcherB.send(executionPlan)).thenReturn("b")
    when(dispatcherC.send(executionPlan)).thenReturn("c")

    val result: String = new AggregateLineageDispatcher(
      dispatcherA,
      Some(Seq(dispatcherB, dispatcherC))).send(executionPlan)
    result mustBe "a"

    verify(dispatcherA, times(1)).send(executionPlan)
    verify(dispatcherB, times(1)).send(executionPlan)
    verify(dispatcherC, times(1)).send(executionPlan)
  }
}
