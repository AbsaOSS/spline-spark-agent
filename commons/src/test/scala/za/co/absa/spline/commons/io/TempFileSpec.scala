/*
 * Copyright 2023 ABSA Group Limited
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

package za.co.absa.spline.commons.io

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI

class TempFileSpec extends AnyFlatSpec with Matchers {
  behavior of "`apply`"

  it should "create a unique temp file" in {
    val path1 = TempFile().deleteOnExit().path
    val path2 = TempFile().deleteOnExit().path

    path1 should not equal path2
    path1.toFile.exists should be(true)
    path2.toFile.exists should be(true)
  }

  it should "create files with prefix and suffix" in {
    val name1 = TempFile("foo").deleteOnExit().path.getFileName.toString
    val name2 = TempFile("", "bar").deleteOnExit().path.getFileName.toString
    val name3 = TempFile("foo", "bar").deleteOnExit().path.getFileName.toString

    name1 should startWith("foo")
    name2 should endWith("bar")
    name3 should (startWith("foo") and endWith("bar"))
  }

  behavior of "`asString`"

  it should "return valid string path" in {
    val tempFile = TempFile(prefix = "fake_tmp_").deleteOnExit()

    tempFile.asString should include("/fake_tmp_")
  }

  behavior of "`toURI`"

  it should "return valid URI" in {
    val tempFile = TempFile(prefix = "fake_tmp_").deleteOnExit()

    Option(tempFile.toURI) should not be empty
    tempFile.toURI shouldBe a[URI]
    tempFile.toURI.toString should startWith("file:")
    tempFile.toURI.toString should include("/fake_tmp_")

  }

}
