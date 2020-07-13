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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.commons.spark.SparkTestBase
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.qualifier.HDFSPathQualifier

import scala.util.{Failure, Try}

class ReadCommandExtractorSpec extends AnyFlatSpec with Matchers with SparkTestBase with MockitoSugar {

  private val conf = new Configuration()
  private val pathQualifier = new HDFSPathQualifier(conf)

  it must "defer to the relation handler if there is a compatible one" in {
    val logicalPlan: LogicalRelation = LogicalRelation(TestRelation(spark.sqlContext))
    object TestRelationHandler extends ReadRelationHandler {
      override def isApplicable(relation: BaseRelation): Boolean = relation.isInstanceOf[TestRelation]

      override def apply(relation: BaseRelation, logicalPlan: LogicalPlan): ReadCommand =
        ReadCommand(SourceIdentifier(Some("test")), logicalPlan)
    }

    val result: Option[ReadCommand] =
      new ReadCommandExtractor(pathQualifier, spark, TestRelationHandler).asReadCommand(logicalPlan)

    result.isDefined mustBe true
    result.map(rc => rc.sourceIdentifier).flatMap(si => si.format).getOrElse("fail") mustBe "test"
  }

  it must "return a None if there is no way to handle the relation" in {
    val relationNotHandled: Boolean =
      Try(
        new ReadCommandExtractor(pathQualifier, spark, NoOpReadRelationHandler())
          .asReadCommand(LogicalRelation(TestRelation(spark.sqlContext)))
      )
      match {
        case _: Failure[_] => true
        case _ => false
      }
    relationNotHandled mustBe true
  }
}

private case class TestRelation(sqlCxt: SQLContext) extends BaseRelation {
  override def sqlContext: SQLContext = sqlCxt

  override def schema: StructType = StructType(Seq.empty)
}
