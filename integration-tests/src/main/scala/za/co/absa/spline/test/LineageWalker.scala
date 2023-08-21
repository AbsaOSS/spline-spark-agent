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

package za.co.absa.spline.test

import za.co.absa.spline.producer.model._

/**
 * A class to walk through the execution plan.
 *
 * @param allOpMap A map of all operations in the execution plan.
 * @param funMap   A map of all functional expressions in the execution plan.
 * @param litMap   A map of all literals in the execution plan.
 * @param attrMap  A map of all attributes in the execution plan.
 */
class LineageWalker(
  allOpMap: Map[String, Operation],
  funMap: Map[String, FunctionalExpression],
  litMap: Map[String, Literal],
  attrMap: Map[String, Attribute]
) {

  /**
   * Retrieves an attribute by its ID.
   *
   * @param attributeId The ID of the attribute.
   * @return The attribute with the given ID.
   */
  def getAttributeById(attributeId: String): Attribute = attrMap(attributeId)

  /**
   * Retrieves an operation by its ID.
   *
   * @param operationId The ID of the operation.
   * @return The operation with the given ID.
   */
  def getOperationById(operationId: String): Operation = allOpMap(operationId)

  /**
   * Checks if an attribute depends on another attribute.
   *
   * @param sourceAttribute The attribute to check.
   * @param targetAttribute The attribute that the first attribute may depend on.
   * @return True if the first attribute depends on the second attribute, false otherwise.
   */
  def dependsOn(sourceAttribute: Attribute, targetAttribute: Attribute): Boolean = {
    dependsOnRecursively(AttrRef(sourceAttribute.id), targetAttribute.id)
  }

  private def dependsOnRecursively(refs: Seq[AttrOrExprRef], attrId: String): Boolean =
    refs.exists(dependsOnRecursively(_, attrId))

  private def dependsOnRecursively(ref: AttrOrExprRef, targetAttrId: String): Boolean = ref match {
    case AttrRef(attrId) =>
      attrId == targetAttrId || dependsOnRecursively(attrMap(attrId).childRefs, targetAttrId)
    case ExprRef(exprId) =>
      funMap.get(exprId).exists(expr => dependsOnRecursively(expr.childRefs, targetAttrId))
  }

}

object LineageWalker {

  /**
   * Creates a LineageWalker instance from an execution plan.
   *
   * @param plan The execution plan.
   * @return A LineageWalker instance.
   */
  def apply(plan: ExecutionPlan): LineageWalker = {
    val allOpMap = plan.operations.all
      .map(op => op.id -> op)
      .toMap

    val funMap = plan.expressions
      .functions
      .map(fun => fun.id -> fun).toMap

    val litMap = plan.expressions
      .constants
      .map(lit => lit.id -> lit).toMap

    val attMap = plan.attributes
      .map(att => att.id -> att).toMap

    new LineageWalker(allOpMap, funMap, litMap, attMap)
  }

}
