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
  override def apply(relation: BaseRelation, logicalPlan: LogicalPlan): ReadCommand =
    throw new UnsupportedOperationException("NoOpRelationHandler - use isApplicable to avoid this exception")
}

object NoOpReadRelationHandler {
  def apply(): ReadRelationHandler = new NoOpReadRelationHandler(new BaseConfiguration)
}
