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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.ModelConstants.CommonExtras
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model.{AttrRef, Attribute, DataOperation, FunctionalExpression}

class MergeIntoNodeBuilder
  (logicalPlan: LogicalPlan)
  (idGenerators: IdGeneratorsBundle, dataTypeConverter: DataTypeConverter, dataConverter: DataConverter, postProcessor: PostProcessor)
  extends GenericPlanNodeBuilder(logicalPlan)(idGenerators, dataTypeConverter, dataConverter, postProcessor) {

  private lazy val mergeInputs: Seq[Seq[Attribute]] = {
    val Seq(srcAttrs, trgAttrs) = inputAttributes
    val srcAttrsByName = srcAttrs.map(a => a.name -> a).toMap
    trgAttrs.map(trg => {
      val maybeSrc = srcAttrsByName.get(trg.name)
      Seq(trg) ++ maybeSrc
    })
  }

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

  override def build(): DataOperation = {

    val condition = extractValue[Any](logicalPlan, "condition").toString
    val matchedClauses = extractValue[Seq[Any]](logicalPlan, "matchedClauses").map(_.toString)
    val notMatchedClauses = extractValue[Seq[Any]](logicalPlan, "notMatchedClauses").map(_.toString)

    val dop = DataOperation(
      id = operationId,
      name = logicalPlan.nodeName,
      childIds = childIds,
      output = outputAttributes.map(_.id),
      params = Map(
        "condition" -> condition,
        "matchedClauses" -> matchedClauses,
        "notMatchedClauses" -> notMatchedClauses),
      extra = Map.empty
    )

    postProcessor.process(dop)
  }
}

object MergeIntoNodeBuilder {

  type DeltaMergeIntoClause = SparkExpression
  type DeltaMergeAction = SparkExpression

  def extractChildren(mergeNode: LogicalPlan): Seq[LogicalPlan] = Seq(extractSource(mergeNode), extractTarget(mergeNode))

  private def extractSource(mergeNode: LogicalPlan): LogicalPlan = extractValue[LogicalPlan](mergeNode, "source")

  private def extractTarget(mergeNode: LogicalPlan): LogicalPlan = extractValue[LogicalPlan](mergeNode, "target")

  private def extractCondition(mergeNode: LogicalPlan): SparkExpression = extractValue[SparkExpression](mergeNode, "condition")

  private def extractMatchedClauses(mergeNode: LogicalPlan): Seq[DeltaMergeIntoClause] = extractValue[Seq[DeltaMergeIntoClause]](mergeNode, "matchedClauses")

  private def extractNonMatchedClauses(mergeNode: LogicalPlan): Seq[DeltaMergeIntoClause] = extractValue[Seq[DeltaMergeIntoClause]](mergeNode, "notMatchedClauses")

  private def extractClauseActions(clause: DeltaMergeIntoClause): Seq[DeltaMergeAction] = extractValue[Seq[DeltaMergeAction]](clause, "actions")

  private def extractActionTargetAttrName(clause: DeltaMergeAction): String = extractValue[Seq[String]](clause, "targetColNameParts").head

  private def extractActionSourceExpression(clause: DeltaMergeAction): SparkExpression = extractValue[SparkExpression](clause, "expr")
}
