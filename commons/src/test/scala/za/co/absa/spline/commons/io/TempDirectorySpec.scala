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

import java.io.File
import java.net.URI


class TempDirectorySpec extends AnyFlatSpec with Matchers {

  behavior of "`apply`"

  it should "create a unique temp directory" in {
    val path1 = TempDirectory().deleteOnExit().path
    val path2 = TempDirectory().deleteOnExit().path

    path1 should not equal path2
    path1.toFile.exists should be(true)
    path2.toFile.exists should be(true)
  }

  it should "create directories with prefix and suffix" in {
    val name1 = TempDirectory("foo").deleteOnExit().path.getFileName.toString
    val name2 = TempDirectory("", "bar").deleteOnExit().path.getFileName.toString
    val name3 = TempDirectory("foo", "bar").deleteOnExit().path.getFileName.toString

    name1 should startWith("foo")
    name2 should endWith("bar")
    name3 should (startWith("foo") and endWith("bar"))
  }

  behavior of "`asString`"

  it should "return valid string path with unix slashes" in {
    val tempDirectory = TempDirectory(prefix = "fake_tmp_").deleteOnExit()

    tempDirectory.asString should include("/fake_tmp_")
  }

  behavior of "`toURI`"

  it should "return valid URI" in {
    val tempDirectory = TempDirectory(prefix = "fake_tmp_").deleteOnExit()

    Option(tempDirectory.toURI) should not be empty
    tempDirectory.toURI shouldBe a[URI]
    tempDirectory.toURI.toString should startWith("file:")
    tempDirectory.toURI.toString should include("/fake_tmp_")
  }

  behavior of "`delete`"

  it should "remove directory with content" in {
    val dir = TempDirectory()
    val subDir = new File(dir.path.toFile, "subdir")
    val testFile = new File(subDir, "testFile")

    subDir.mkdir()
    testFile.createNewFile()

    dir.delete()

    testFile.exists should be(false)
    subDir.exists should be(false)
    dir.path.toFile.exists should be(false)
  }

}
