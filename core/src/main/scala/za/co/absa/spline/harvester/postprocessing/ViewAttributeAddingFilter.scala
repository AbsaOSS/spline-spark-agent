/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.postprocessing

import org.apache.commons.configuration.Configuration
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.producer.model.v1_1.{AttrOrExprRef, DataOperation, ExecutionPlan}

class ViewAttributeAddingFilter(conf: Configuration) extends AbstractPostProcessingFilter {

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = {
    val views = plan.operations.other
      .map(_.filter(_.name.get.startsWith("View")))
      .getOrElse(Seq.empty)

    if (views.isEmpty)
      plan
    else
      addMissingAttributeLinks(plan, views)
  }

  private def addMissingAttributeLinks(plan: ExecutionPlan, views: Seq[DataOperation]): ExecutionPlan = {
    val newAttributeRefsSeq = views.map { v =>
      // assume views can have only one child
      val childId = v.childIds.get.head
      val child = plan.operations.other.getOrElse(Seq.empty).find(_.id == childId).get

      if(!deepEq(v.output, child.output)) {
        newAttributeReferences(v.output.get, child.output.get)
      } else {
        Map.empty[String, String]
      }
    }

    val attributeReferences = newAttributeRefsSeq.reduce(_ ++ _)

    plan.copy(
      attributes = plan.attributes.map{ attSeq =>
        attSeq.map{ att =>
          if (attributeReferences.contains(att.id)) {
            att.copy(childRefs = Some(Seq(AttrOrExprRef(Some(attributeReferences(att.id)), None))))
          } else {
            att
          }
        }
      }
    )
  }

  private def newAttributeReferences(viewOutput: Seq[String], childOutput: Seq[String]): Map[String, String] = {
    viewOutput.zip(childOutput)
      .filter{ case (v, ch) => v != ch}
      .toMap
  }

  private def deepEq(left: Option[Seq[String]], right: Option[Seq[String]]): Boolean = (left, right) match {
    case (None, None) => true
    case (None, Some(_)) => false
    case (Some(lSeq), Some(rSeq)) => deepEq(lSeq, rSeq)
  }

  private def deepEq(left: Seq[String], right: Seq[String]): Boolean = {
    if(left.size != right.size) {
      false
    } else {
      !left.zip(right).exists { case (l, r) => l != r}
    }
  }

}
