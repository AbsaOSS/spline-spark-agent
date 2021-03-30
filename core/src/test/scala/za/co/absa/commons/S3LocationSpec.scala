package za.co.absa.commons

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import S3Location.StringS3LocationExt

class S3LocationSpec extends AnyFlatSpec with Matchers {

  "StringS3LocationExt" should "parse S3 path from String using toS3Location" in {
    "s3://mybucket-123/path/to/file.ext".toS3Location shouldBe Some(SimpleS3Location("s3", "mybucket-123", "path/to/file.ext"))
    "s3n://mybucket-123/path/to/ends/with/slash/".toS3Location shouldBe Some(SimpleS3Location("s3n", "mybucket-123", "path/to/ends/with/slash/"))
    "s3a://mybucket-123.asdf.cz/path-to-$_file!@#$.ext".toS3Location shouldBe Some(SimpleS3Location("s3a", "mybucket-123.asdf.cz", "path-to-$_file!@#$.ext"))
  }

  it should "find no valid S3 path when parsing invalid S3 path from String using toS3Location" in {
    "s3x://mybucket-123/path/to/file/on/invalid/prefix".toS3Location shouldBe None
    "s3://bb/some/path/but/bucketname/too/short".toS3Location shouldBe None
  }

  it should "check path using isValidS3Path" in {
    "s3://mybucket-123/path/to/file.ext".isValidS3Path shouldBe true
    "s3n://mybucket-123/path/to/ends/with/slash/".isValidS3Path shouldBe true
    "s3a://mybucket-123.asdf.cz/path-to-$_file!@#$.ext".isValidS3Path shouldBe true

    "s3x://mybucket-123/path/to/file/on/invalid/prefix".isValidS3Path shouldBe false
    "s3://bb/some/path/but/bucketname/too/short".isValidS3Path shouldBe false
  }

}