package za.co.absa.spline.harvester.builder.read

import com.databricks.spark.xml.XmlRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.util.{Failure, Try}

class NoOpReadRelationHandlerSpec extends AnyFlatSpec
                                          with Matchers
                                          with MockitoSugar {

  it should "unconditionally return false for isApplicable" in {
    val handler: ReadRelationHandler = NoOpReadRelationHandler()
    handler.isApplicable(mock[BaseRelation]) mustBe false
    handler.isApplicable(mock[XmlRelation]) mustBe false
  }

  it should "throw an exception if applied" in {
    val operationFailed: Boolean = Try(NoOpReadRelationHandler().apply(mock[BaseRelation],
                                                                           mock[LogicalPlan])) match {
      case _: Failure[_] => true
      case _ => false
    }
    operationFailed mustBe true
  }
}
