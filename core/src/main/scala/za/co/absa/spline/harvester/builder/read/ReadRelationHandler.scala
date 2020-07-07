/*
 * Copyright 2020 ABSA Group Limited
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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation

/**
 * Extension point for dealing with [[BaseRelation]] instances not
 * already handled within Spline.
 *
 * Implementations of this trait should have a constructor that takes a
 * [[org.apache.commons.configuration.Configuration]] as a parameter.
 */
trait ReadRelationHandler extends ((BaseRelation, LogicalPlan) => ReadCommand)  {

  /**
   * Determine if the relation can be processed by implementing classes.
   *
   * @param relation the relation to process
   * @return true iff the implementing class can process this relation
   */
  def isApplicable(relation: BaseRelation): Boolean

  /**
   * Process the relation in some arbitrary fashion.
   *
   * @param relation the relation to process
   * @param logicalPlan the plan associated with the relation
   * @return a [[ReadCommand]]
   */
  def apply(relation: BaseRelation,
            logicalPlan: LogicalPlan): ReadCommand
}
