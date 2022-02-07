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
  opMap: Map[String, DataOperation],
  funMap: Map[String, FunctionalExpression],
  litMap:  Map[String, Literal],
  attrMap: Map[String, Attribute]
) {

  def op(write: WriteOperation): WriteWalker = new WriteWalker(write)
  def op(op: DataOperation): OperationWalker = new OperationWalker(op)
  def att(attributeId: String): AttributeWalker = new AttributeWalker(attrMap(attributeId))


  class WriteWalker(write: WriteOperation) {
    def precedingOp: DataOperation = {
      assert(write.childIds.size == 1)
      opMap(write.childIds(0))
    }

    def precedingOps: Seq[DataOperation] = {
      write.childIds.map(opMap)
    }
  }

  class OperationWalker(op: DataOperation) {
    def precedingOp: DataOperation = {
      assert(op.childIds.get.size == 1)
      opMap(op.childIds.get(0))
    }

    def precedingOps: Seq[DataOperation] = {
      op.childIds.get.map(opMap)
    }
  }

  class AttributeWalker(current: Attribute) {
    def dependsOn(id: String): Boolean = {
      dependsOnRec(AttrOrExprRef(Some(current.id), None), id)
    }
  }

  private def dependsOnRec(maybeRefs: Option[Seq[AttrOrExprRef]], id: String): Boolean =
    maybeRefs
      .map(_.exists(dependsOnRec(_, id)))
      .getOrElse(false)

  private def dependsOnRec(ref: AttrOrExprRef, id: String): Boolean = ref match {
    case AttrOrExprRef(Some(attrIfd), _) =>
      if(attrIfd == id) true
      else dependsOnRec(attrMap(attrIfd).childRefs, id)
    case AttrOrExprRef(_, Some(exprId)) =>
      if(exprId == id) true
      else {
        if (litMap.contains("exprId")) false
        else dependsOnRec(funMap(exprId).childRefs, id)
      }
  }
  
}

object LineageWalker {

  def apply(plan: ExecutionPlan): LineageWalker = {
    val opMap = plan.operations.other
      .map(opSeqToMap(_))
      .getOrElse(Map.empty)

    val funMap = plan.expressions
      .flatMap(_.functions)
      .map(_.map(fun => fun.id -> fun).toMap)
      .getOrElse(Map.empty)

    val litMap =  plan.expressions
      .flatMap(_.constants)
      .map(_.map(lit => lit.id -> lit).toMap)
      .getOrElse(Map.empty)

    val attMap = plan.attributes
      .map(_.map(att => att.id -> att).toMap)
      .getOrElse(Map.empty)

    new LineageWalker(opMap, funMap, litMap, attMap)

  }

  private def opSeqToMap(ops: Seq[DataOperation]): Map[String, DataOperation] =
    ops.map(op => op.id -> op).toMap


}
