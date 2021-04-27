package za.co.absa.spline.harvester.postprocessing

import org.apache.spark.internal.Logging
import za.co.absa.commons.HierarchicalObjectFactory
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.harvester.postprocessing.CompositePostProcessingFilter.FiltersKey
import za.co.absa.spline.producer.model.v1_1._

class CompositePostProcessingFilter(delegatees: Seq[PostProcessingFilter])
  extends PostProcessingFilter
    with Logging {

  def this(objectFactory: HierarchicalObjectFactory) = this(
    objectFactory.createComponentsByKey(FiltersKey)
  )

  override def processExecutionEvent(event: ExecutionEvent, ctx: HarvestingContext): ExecutionEvent = chainCall(event) {
    _.processExecutionEvent(_, ctx)
  }

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = chainCall(plan) {
    _.processExecutionPlan(_, ctx)
  }

  override def processReadOperation(op: ReadOperation, ctx: HarvestingContext): ReadOperation = chainCall(op) {
    _.processReadOperation(_, ctx)
  }

  override def processWriteOperation(op: WriteOperation, ctx: HarvestingContext): WriteOperation = chainCall(op) {
    _.processWriteOperation(_, ctx)
  }

  override def processDataOperation(op: DataOperation, ctx: HarvestingContext): DataOperation = chainCall(op) {
    _.processDataOperation(_, ctx)
  }

  private def chainCall[A](arg: A)(call: (PostProcessingFilter, A) => A): A = {
    delegatees.foldLeft(arg) {
      case (z, d) => call(d, z)
    }
  }
}

object CompositePostProcessingFilter {
  private val FiltersKey = "filters"
}
