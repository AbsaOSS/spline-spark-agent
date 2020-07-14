package za.co.absa.spline.harvester.dispatcher.hdfsdispatcher.naming

import org.apache.commons.configuration.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.producer.model.ExecutionPlan

class DefaultNamingStrategySpec extends AnyFlatSpec with MockitoSugar with Matchers {

  it should "return exactly the base name given" in {
    new DefaultNamingStrategy(mock[Configuration]).apply(mock[ExecutionPlan], "foo") shouldBe "foo"
  }
}
