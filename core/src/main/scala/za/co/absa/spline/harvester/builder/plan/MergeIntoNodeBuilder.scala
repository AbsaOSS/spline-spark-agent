/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.spline.harvester.builder.plan

import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.ModelConstants.CommonExtras
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.plugin.embedded.DeltaPlugin
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.{AttrRef, Attribute, FunctionalExpression}

class MergeIntoNodeBuilder
(override val logicalPlan: DeltaPlugin.SyntheticDeltaMerge)
  (idGenerators: IdGeneratorsBundle, dataTypeConverter: DataTypeConverter, dataConverter: DataConverter, postProcessor: PostProcessor)
  extends GenericPlanNodeBuilder(logicalPlan)(idGenerators, dataTypeConverter, dataConverter, postProcessor) {

  private lazy val mergeInputs: Seq[Seq[Attribute]] = inputAttributes.transpose

  override lazy val functionalExpressions: Seq[FunctionalExpression] = Seq.empty

  override lazy val outputAttributes: Seq[Attribute] =
    mergeInputs.map(constructMergeAttribute)

  private def constructMergeAttribute(attributes: Seq[Attribute]) = {
    val attr1 = attributes.head
    val idRefs = attributes.map(a => AttrRef(a.id))
    Attribute(
      id = idGenerators.attributeIdGenerator.nextId(),
      dataType = attr1.dataType,
      childRefs = idRefs,
      extra = Map(CommonExtras.Synthetic -> true),
      name = attr1.name
    )
  }
}
