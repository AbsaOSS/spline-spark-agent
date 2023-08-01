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

package za.co.absa.commons.buildinfo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BuildInfoSpec extends AnyFlatSpec with Matchers {

  "Version" should "return build version" in {
    BuildInfo.Version should equal("4.2.42-TEST")
  }

  "Timestamp" should "return build timestamp" in {
    BuildInfo.Timestamp should equal("1234567890")
  }

  "BuildProps" should "return build info as Java properties" in {
    BuildInfo.BuildProps.getProperty("build.version") should equal("4.2.42-TEST")
    BuildInfo.BuildProps.getProperty("build.timestamp") should equal("1234567890")
  }

  behavior of "Customizable BuildInfo"

  it should "read from a custom resource" in {
    object MyBuildInfo extends BuildInfo(resourcePrefix = "/buildinfo-test/my")
    MyBuildInfo.Version should equal("My version")
  }

  it should "support custom property mapping" in {
    object MyBuildInfo extends BuildInfo(propMapping = PropMapping(
      version = "bld.ver",
      timestamp = "bld.ttt"
    ))
    MyBuildInfo.Version should equal("Custom version")
    MyBuildInfo.Timestamp should equal("Custom timestamp")
  }

  it should "provide apply() method" in {
    BuildInfo(resourcePrefix = "/buildinfo-test/my").Version should equal("My version")
    BuildInfo(propMapping = PropMapping(version = "bld.ver")).Version should equal("Custom version")
  }
}
