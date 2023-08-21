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

import za.co.absa.spline.harvester.builder.OperationNodeBuilder.IOAttributes
import za.co.absa.spline.producer.model._

import scala.annotation.tailrec

object ProducerModelImplicits {

  implicit class OperationOps(val operation: Operation) extends AnyVal {

    def outputAttributes(implicit walker: LineageWalker): IOAttributes = {
      operation.output.map(walker.getAttributeById)
    }

    def childOperations(implicit walker: LineageWalker): Seq[Operation] = {
      operation.childIds.map(walker.getOperationById)
    }

    def childOperation(implicit walker: LineageWalker): Operation = {
      val Seq(child) = childOperations(walker)
      child
    }

    def dagLeaves(implicit walker: LineageWalker): (Seq[ReadOperation], Seq[DataOperation]) =
      findLeaves(operation)


  }

  private def findLeaves(operation: Operation)(implicit walker: LineageWalker): (Seq[ReadOperation], Seq[DataOperation]) = {

    @tailrec
    def findLeavesRec(
      toVisit: Seq[Operation],
      readLeaves: List[ReadOperation],
      dataLeaves: List[DataOperation]
    ): (Seq[ReadOperation], Seq[DataOperation]) = toVisit match {
      case (head: ReadOperation) +: tail =>
        findLeavesRec(tail, head :: readLeaves, dataLeaves)

      case (head: DataOperation) +: tail =>
        val children = head.childOperations
        if (children.isEmpty) {
          findLeavesRec(tail, readLeaves, head :: dataLeaves)
        } else {
          findLeavesRec(children ++ tail, readLeaves, dataLeaves)
        }

      case Seq() => (readLeaves, dataLeaves)
    }

    val initialToVisit = operation match {
      case write: WriteOperation => write.childOperations
      case _ => Seq(operation)
    }

    findLeavesRec(initialToVisit, Nil, Nil)
  }

}
