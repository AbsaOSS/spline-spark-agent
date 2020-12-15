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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.producer.model.v1_1.WriteOperation

class DataSourceUriLineageFilterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  val harvestingContextMock = mock[HarvestingContext]

  it should "apply one filter" in {
    val wop = WriteOperation(
      Nil,
      "" +
        "jdbc:sqlserver://database.windows.net:1433" +
        ";user=sample" +
        ";password=123456" + //NOSONAR
        ";encrypt=true" +
        ";trustServerCertificate=false" +
        ";hostNameInCertificate=*.database.windows.net" +
        ";loginTimeout=30:",
      append = false, "", Nil, None, None)

    val filter = new DataSourceUriLineageFilter()
    val filteredOp = filter.processWriteOperation(wop, harvestingContextMock)

    filteredOp.outputSource shouldEqual "" +
      "jdbc:sqlserver://database.windows.net:1433" +
      ";user=sample" +
      ";password=*****" + //NOSONAR <-- PASSWORD SHOULD BE SANITIZED
      ";encrypt=true" +
      ";trustServerCertificate=false" +
      ";hostNameInCertificate=*.database.windows.net" +
      ";loginTimeout=30:"
  }

}
