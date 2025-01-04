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

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.harvester.IdGeneratorsBundle
import za.co.absa.spline.harvester.ModelConstants.CommonExtras
import za.co.absa.spline.harvester.builder.OperationNodeBuilder.IOAttributes
import za.co.absa.spline.harvester.builder.plan.MergeIntoNodeBuilder._
import za.co.absa.spline.harvester.converter.{DataConverter, DataTypeConverter}
import za.co.absa.spline.harvester.postprocessing.PostProcessor
import za.co.absa.spline.producer.model._

class MergeIntoNodeBuilder
  (logicalPlan: LogicalPlan)
  (idGenerators: IdGeneratorsBundle, dataTypeConverter: DataTypeConverter, dataConverter: DataConverter, postProcessor: PostProcessor)
  extends GenericPlanNodeBuilder(logicalPlan)(idGenerators, dataTypeConverter, dataConverter, postProcessor) {

  private lazy val target: LogicalPlan = extractTarget(logicalPlan)
  private lazy val trgAttrs: IOAttributes = target.output.map(attributeConverter.convert)

  private lazy val dependenciesByAttrName: Map[String, Seq[AttrOrExprRef]] =
    (extractMatchedClauses(logicalPlan) ++ extractNonMatchedClauses(logicalPlan))
      .flatMap(extractClauseActions)
      .map((a: DeltaMergeAction) => {
        val trgAttrName = extractActionTargetAttrName(a)
        val srcExpr = extractActionSourceExpression(a)
        trgAttrName -> exprToRefConverter.convert(srcExpr)
      })
      .groupBy { case (attrName, _) => attrName }
      .mapValues(_.map({ case (_, ref) => ref }).distinct)

  private lazy val syntheticFunctionalExprs: Seq[FunctionalExpression] = trgAttrs.map(buildFunctionalExpression)

  override lazy val functionalExpressions: Seq[FunctionalExpression] = syntheticFunctionalExprs ++ expressionConverter.values

  override lazy val outputAttributes: IOAttributes = trgAttrs.zip(syntheticFunctionalExprs).map { case (trgAttr, functionalExpr) =>
      buildOutputAttribute(trgAttr, functionalExpr)
  }

  override def build(): DataOperation = {
    val conditionStr = extractCondition(logicalPlan).toString
    val matchedClausesStr = extractMatchedClauses(logicalPlan).map(_.toString)
    val notMatchedClausesStr = extractNonMatchedClauses(logicalPlan).map(_.toString)

    val dop = DataOperation(
      id = operationId,
      name = logicalPlan.nodeName,
      childIds = childIds,
      output = outputAttributes.map(_.id),
      params = Map(
        "condition" -> conditionStr,
        "matchedClauses" -> matchedClausesStr,
        "notMatchedClauses" -> notMatchedClausesStr),
      extra = Map.empty
    )

    postProcessor.process(dop)
  }

  private def buildFunctionalExpression(targetAttribute: Attribute): FunctionalExpression = {
    val targetRef = AttrRef(targetAttribute.id)
    val sourceRefs = dependenciesByAttrName(targetAttribute.name)
    FunctionalExpression(
      id = idGenerators.expressionIdGenerator.nextId(),
      dataType = targetAttribute.dataType,
      childRefs = targetRef +: sourceRefs,
      extra = Map(CommonExtras.Synthetic -> true),
      name = MergeIntoNodeBuilder.SyntheticFunctionName,
      params = Map.empty
    )
  }

  private def buildOutputAttribute(targetAttribute: Attribute, function: FunctionalExpression): Attribute = {
    Attribute(
      id = idGenerators.attributeIdGenerator.nextId(),
      dataType = function.dataType,
      childRefs = List(ExprRef(function.id)),
      extra = Map(CommonExtras.Synthetic -> true),
      name = targetAttribute.name
    )
  }
}

object MergeIntoNodeBuilder {

  type DeltaMergeIntoClause = SparkExpression
  type DeltaMergeAction = SparkExpression

  private val SyntheticFunctionName: String = "Merge"

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
