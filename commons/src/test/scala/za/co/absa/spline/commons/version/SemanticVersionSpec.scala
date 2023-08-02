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

package za.co.absa.spline.commons.version

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.commons.version.Version._

import scala.Ordering.Implicits._
import scala.util.Random

class SemanticVersionSpec extends AnyFlatSpec with Matchers {

  behavior of "Version parser"

  it should "parse semantic version" in {
    Version.asSemVer("1.22.333") should be(Version(
      NumericComponent(1),
      NumericComponent(22),
      NumericComponent(333)
    ))
    Version.asSemVer("1.22.333-SNAPSHOT.77") should be(Version(
      NumericComponent(1),
      NumericComponent(22),
      NumericComponent(333),
      PreReleaseComponent(
        StringComponent("SNAPSHOT"),
        NumericComponent(77))
    ))
    Version.asSemVer("1.22.333+foo.bar") should be(Version(
      NumericComponent(1),
      NumericComponent(22),
      NumericComponent(333),
      BuildMetadataComponent(
        StringComponent("foo"),
        StringComponent("bar"))
    ))
    Version.asSemVer("1.22.333-SNAPSHOT.77+foo.bar") should be(Version(
      NumericComponent(1),
      NumericComponent(22),
      NumericComponent(333),
      PreReleaseComponent(
        StringComponent("SNAPSHOT"),
        NumericComponent(77)),
      BuildMetadataComponent(
        StringComponent("foo"),
        StringComponent("bar"))
    ))
  }

  // bugfix: commons-14
  it should "handle big numbers" in {
    Version.asSemVer("1.0.0+20130313144700") should be(Version(
      NumericComponent(1),
      NumericComponent(0),
      NumericComponent(0),
      BuildMetadataComponent(
        NumericComponent(20130313144700L))
    ))
  }

  it should "not parse" in {
    intercept[IllegalArgumentException](Version.asSemVer(""))
    intercept[IllegalArgumentException](Version.asSemVer("1"))
    intercept[IllegalArgumentException](Version.asSemVer("1.1"))
    intercept[IllegalArgumentException](Version.asSemVer("1.1.0."))
    intercept[IllegalArgumentException](Version.asSemVer("1.1.01"))
    intercept[IllegalArgumentException](Version.asSemVer("1.1.1,"))
    intercept[IllegalArgumentException](Version.asSemVer("1.1.1+@"))
    intercept[IllegalArgumentException](Version.asSemVer("1.1.1-$"))
    intercept[IllegalArgumentException](Version.asSemVer("1.1.1-"))
    intercept[IllegalArgumentException](Version.asSemVer("a.b.c"))
  }

  behavior of "Version ordering"

  it should "compare" in {
    (semver"0.0.0" < semver"0.0.1") should be(true)
    (semver"0.0.0" < semver"0.1.0") should be(true)
    (semver"0.0.0" < semver"1.0.0") should be(true)
    (semver"1.2.3" < semver"3.2.1") should be(true)
    (semver"1.1.1-beta" < semver"1.1.1") should be(true)
    (semver"1.1.1+meta" equiv semver"1.1.1") should be(true)
    (semver"1.1.1-beta+meta" equiv semver"1.1.1-beta") should be(true)
    (semver"1.1.1-beta" < semver"1.1.1-beta.1") should be(true)
    (semver"1.1.1-beta2" > semver"1.1.1-beta.1") should be(true)
    (semver"1.1.1-beta2" > semver"1.1.1-beta11") should be(true)
    (semver"1.1.1-beta.2" < semver"1.1.1-beta.11") should be(true)
    (semver"1.1.1-beta.1" < semver"1.1.1-beta.a") should be(true)
    (semver"1.1.1-beta.1" < semver"1.1.1-beta.1a") should be(true)
    (semver"1.1.1-beta.a" < semver"1.1.1-beta.b") should be(true)
    (semver"1.1.1-B" < semver"1.1.1-b") should be(true)
  }

  it should "sort" in {
    val versions = Seq(
      semver"0.0.0+meta",
      semver"0.0.1",
      semver"0.0.11",
      semver"0.1.1-alpha+m.e.t.a",
      semver"0.1.1-beta",
      semver"0.1.1-beta.2",
      semver"0.1.1-beta.11",
      semver"0.1.1-beta11",
      semver"0.1.1-beta2",
      semver"0.1.1"
    )
    Random.shuffle(versions).sorted should equal(versions)
  }

  behavior of "asString()"

  it should "render the original version string" in {
    val versionStrings = Seq(
      "1.22.333",
      "1.22.333-beta",
      "1.22.333-beta.42",
      "1.22.333+meta",
      "1.22.333+meta.42",
      "1.22.333-beta+meta",
      "1.22.333-beta.42+meta",
      "1.22.333-beta+meta.42",
      "1.22.333-beta.42+meta.42",
      "1.22.333-b.e.t.a+m.e.t.a"
    )
    versionStrings.foreach(verStr => Version.asSemVer(verStr).asString should equal(verStr))
  }

  behavior of "SemVer specific operations"

  "major(), minor(), patch()" should "return respective components" in {
    semver"1.22.333".major should be(1)
    semver"1.22.333".minor should be(22)
    semver"1.22.333".patch should be(333)
  }

  "core()" should "return a core version component (major.minor.patch) as a SemanticVersion" in {
    val versions = Seq(
      semver"1.22.333",
      semver"1.22.333-beta",
      semver"1.22.333+meta",
      semver"1.22.333-beta+meta"
    )
    versions.foreach(version => {
      val coreVersion = version.core
      coreVersion should equal(semver"1.22.333")
      coreVersion.core should be theSameInstanceAs coreVersion
    })
  }

  "preRelease()" should "return a pre-release component as a Version" in {
    semver"1.2.3-beta.4+meta.5".preRelease should equal(Some(ver"beta.4"))
    semver"1.2.3+meta.5".preRelease should not be defined
    semver"1.2.3".preRelease should not be defined
  }

  "buildMeta()" should "return a build-meta component as a Version" in {
    semver"1.2.3-beta.4+meta.5".buildMeta should equal(Some(ver"meta.5"))
    semver"1.2.3-beta.4".buildMeta should not be defined
    semver"1.2.3".buildMeta should not be defined
  }
}
