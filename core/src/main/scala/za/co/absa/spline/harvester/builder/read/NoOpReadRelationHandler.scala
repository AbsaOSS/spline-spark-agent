package za.co.absa.spline.harvester.builder.read
import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation

/**
 * No-op [[ReadRelationHandler]], isApplicable always returns false.
 */
class NoOpReadRelationHandler(config: Configuration) extends ReadRelationHandler {
  /**
   * Determine if the relation can be processed by implementing classes.
   *
   * @param relation the relation to process
   * @return true iff the implementing class can process this relation
   */
  override def isApplicable(relation: BaseRelation): Boolean = false

  /**
   * Unconditionally throws an [[UnsupportedOperationException]].
   */
  override def apply(relation: BaseRelation,
                     logicalPlan: LogicalPlan): ReadCommand =
    throw new UnsupportedOperationException("NoOpRelationHandler - use isApplicable to avoid this exception")
}

object NoOpReadRelationHandler {
  def apply(): ReadRelationHandler = new NoOpReadRelationHandler(new BaseConfiguration)
}