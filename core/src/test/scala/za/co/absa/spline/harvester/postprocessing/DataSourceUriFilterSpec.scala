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

class DataSourceUriFilterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  it should "apply one filter" in {

    def uri(pwd: String) = s"jdbc:sqlserver://database.windows.net:1433;user=sample;password=$pwd;" +
      "encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30:"

    val wop = WriteOperation(List.empty, uri("123456"), false, "42", List.empty, None, None)

    val filter = new DataSourceUriFilter()
    val pp = new PostProcessor(Seq(filter), mock[HarvestingContext])

    val filteredOp = pp.process(wop)

    filteredOp.outputSource shouldEqual uri("*****")
  }

}