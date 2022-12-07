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

class LineageWalker(
  allOpMap: Map[String, Operation],
  funMap: Map[String, FunctionalExpression],
  litMap:  Map[String, Literal],
  attrMap: Map[String, Attribute]
) {

  def attributeById(attributeId: String): Attribute = attrMap(attributeId)

  def operationById(operationId: String): Operation = allOpMap(operationId)

  def dependsOn(att: Attribute, onAtt: Attribute): Boolean = {
    dependsOnRec(AttrRef(att.id), onAtt.id)
  }

  private def dependsOnRec(refs: Seq[AttrOrExprRef], id: String): Boolean =
    refs.exists(dependsOnRec(_, id))

  private def dependsOnRec(ref: AttrOrExprRef, id: String): Boolean = ref match {
    case AttrRef(attrIfd) =>
      if(attrIfd == id) true
      else dependsOnRec(attrMap(attrIfd).childRefs, id)
    case ExprRef(exprId) =>
      if(exprId == id) true
      else {
        if (litMap.contains("exprId")) false
        else dependsOnRec(funMap(exprId).childRefs, id)
      }
  }
  
}

object LineageWalker {

  def apply(plan: ExecutionPlan): LineageWalker = {
    val allOpMap = plan.operations.all
      .map(op => op.id -> op)
      .toMap

    val funMap = plan.expressions
      .functions
      .map(fun => fun.id -> fun).toMap

    val litMap =  plan.expressions
      .constants
      .map(lit => lit.id -> lit).toMap

    val attMap = plan.attributes
      .map(att => att.id -> att).toMap

    new LineageWalker(allOpMap, funMap, litMap, attMap)
  }

}
