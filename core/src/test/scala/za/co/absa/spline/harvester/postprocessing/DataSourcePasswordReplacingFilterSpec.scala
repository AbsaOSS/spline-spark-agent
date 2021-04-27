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

package za.co.absa.spline.harvester.postprocessing

import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.producer.model.v1_1.WriteOperation

class DataSourcePasswordReplacingFilterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val defaultProperties =
    new PropertiesConfiguration(getClass.getResource("/spline.default.properties"))
      .subset("spline.postProcessingFilter.dsUriPasswordReplace")

  private val ctxMock = mock[HarvestingContext]

  it should "mask secret in URL query parameter as `password=*****`" in { //NOSONAR
    val wop = WriteOperation(
      "" +
        "jdbc:sqlserver://database.windows.net:1433" +
        ";user=sample" +
        ";password=12345" + //NOSONAR
        ";password=" + //NOSONAR -- empty password is also a password
        ";encrypt=true" +
        ";trustServerCertificate=false" +
        ";hostNameInCertificate=*.database.windows.net" +
        ";loginTimeout=30:",
      append = false, "", None, Nil, None, None)

    val filter = new DataSourcePasswordReplacingFilter(defaultProperties)
    val filteredOp = filter.processWriteOperation(wop, ctxMock)

    filteredOp.outputSource shouldEqual "" +
      "jdbc:sqlserver://database.windows.net:1433" +
      ";user=sample" +
      ";password=*****" + //NOSONAR <-- PASSWORD SHOULD BE SANITIZED
      ";password=*****" + //NOSONAR <-- PASSWORD SHOULD BE SANITIZED
      ";encrypt=true" +
      ";trustServerCertificate=false" +
      ";hostNameInCertificate=*.database.windows.net" +
      ";loginTimeout=30:"
  }

  it should "mask secret in URL userinfo as `user:*****@host`" in {
    val wop1 = WriteOperation("mongodb://bob:super_secret@mongodb.host.example.org:27017?authSource=admin", append = false, "", None, Nil, None, None) //NOSONAR
    val wop2 = WriteOperation("mongodb://bob:@mongodb.host.example.org:27017?authSource=admin", append = false, "", None, Nil, None, None)

    val filter = new DataSourcePasswordReplacingFilter(defaultProperties)

    filter.processWriteOperation(wop1, ctxMock).outputSource shouldEqual "mongodb://bob:*****@mongodb.host.example.org:27017?authSource=admin" //NOSONAR
    filter.processWriteOperation(wop2, ctxMock).outputSource shouldEqual "mongodb://bob:*****@mongodb.host.example.org:27017?authSource=admin" //NOSONAR
  }

}
