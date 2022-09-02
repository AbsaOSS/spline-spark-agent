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

import scala.annotation.tailrec

object ProducerModelImplicits {

  implicit class DataOperationOps(val dataOperation: DataOperation) extends AnyVal {

    def outputAttributes(implicit walker: LineageWalker): Seq[Attribute] = {
      dataOperation.output
        .getOrElse(Seq.empty)
        .map(walker.attributeById)
    }

    def precedingDataOp(implicit walker: LineageWalker): DataOperation = {
      val children = walker.precedingDataOps(dataOperation)
      assert(children.size == 1)
      children.head
    }

    def precedingDataOps(implicit walker: LineageWalker): Seq[DataOperation] =
      walker.precedingDataOps(dataOperation)

    def dagLeafs(implicit walker: LineageWalker): (Seq[ReadOperation], Seq[DataOperation]) =
      findLeaves(dataOperation)

  }

  implicit class WriteOperationOps(val write: WriteOperation) extends AnyVal {

    def precedingDataOp(implicit walker: LineageWalker): DataOperation = {
      val children = walker.precedingDataOps(write)
      assert(children.size == 1)
      children.head
    }

    def precedingDataOps(implicit walker: LineageWalker): Seq[DataOperation] =
      walker.precedingDataOps(write)

    def dagLeaves(implicit walker: LineageWalker): (Seq[ReadOperation], Seq[DataOperation]) =
      findLeaves(write)
  }

  implicit class ReadOperationOps(val readOperation: ReadOperation) extends AnyVal {

    def outputAttributes(implicit walker: LineageWalker): Seq[Attribute] = {
      readOperation.output
        .getOrElse(Seq.empty)
        .map(walker.attributeById)
    }
  }

  private def findLeaves(operation: Operation)(implicit walker: LineageWalker): (Seq[ReadOperation], Seq[DataOperation]) = {

    @tailrec
    def findLeavesRec(
      toVisit: List[Operation],
      readLeaves: List[ReadOperation],
      dataLeaves: List[DataOperation]
    ): (Seq[ReadOperation], Seq[DataOperation]) = toVisit match {
      case (head: ReadOperation) :: tail =>
        findLeavesRec(tail, head :: readLeaves, dataLeaves)

      case (head: DataOperation) :: tail =>
        val children = walker.precedingOps(head)
        if (children.isEmpty) {
          findLeavesRec(tail, readLeaves, head :: dataLeaves)
        } else {
          findLeavesRec(children.toList ++ tail, readLeaves, dataLeaves)
        }

      case Nil => (readLeaves, dataLeaves)
    }

    val initialToVisit = operation match {
      case write: WriteOperation => walker.precedingOps(write).toList
      case _ => operation :: Nil
    }

    findLeavesRec(initialToVisit, Nil, Nil)
  }

}
