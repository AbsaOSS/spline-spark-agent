/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.commons.s3

import scala.util.matching.Regex

trait S3Location {
  def protocol: String
  def bucketName: String  // note that this can represent a bucket name or S3 Access Point Alias, which is an alias to the bucket name
  def path: String

  /**
   * Returns formatted S3 string, e.g. `s3a://myBucket/path/to/somewhere` that
   * [[za.co.absa.commons.s3.SimpleS3Location.SimpleS3LocationExt.toSimpleS3Location]] parses from
   *
   * @return formatted s3 string
   */
  def asSimpleS3LocationString: String = s"$protocol://$bucketName/$path"

  /**
   * Some utils might require specific protocol, or for some operations it might make sense to use specific protocol.
   * These can be helpful to quickly get desired location as a string.
   *
   * @return @return formatted s3 string with concrete protocol
   */
  def asS3LocationString: String = s"s3://$bucketName/$path"
  def asS3ALocationString: String = s"s3a://$bucketName/$path"
}

object SimpleS3Location {

  /**
   * Generally usable regex for validating S3 path, e.g. `s3://my-cool-bucket1/path/to/file/on/s3.txt`
   * S3 path can be represented by a S3 bucket name or S3 Access Point Alias that can be used as a reference to a bucket.
   * Protocols `s3`, `s3n`, and `s3a` are allowed.
   *
   * - Bucket naming rules are defined at
   *    [[https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules]]
   * - S3 Access Point naming rules are defined at
   *    [[https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-access-points.html]]
   *
   * Note: this regexp is not 100% strict and exact, according to the AWS S3 naming rules above. That would require
   *   something more powerful than just a regexp. Some differences worth mentioning:
   *
   *   - The dot character is allowed in S3 bucket name, but not in S3 Access Point. The use of dots in bucket name
   *     is rare, but we decided to keep it in the regexp - so that we have a pattern that satisfies both,
   *     access point and bucket name, but it's not as strict as it could be.
   *
   *   - Maximum length of S3 Access Point is 50, and max length of S3 bucket name is 63. We've
   *     decided to ignore this detail and use 63.
   *
   *   - No check for first and last character (or prefix/suffix) in a bucket name.
   */
  private val S3LocationRx: Regex = "^(s3[an]?)://([-a-z0-9.]{3,63})/(.*)$".r

  def apply(path: String): SimpleS3Location = {
    path.toSimpleS3Location.getOrElse(throw new IllegalArgumentException(s"Could not parse S3 location from $path!"))
  }

  implicit class SimpleS3LocationExt(val path: String) extends AnyVal {

    def toSimpleS3Location: Option[SimpleS3Location] = PartialFunction.condOpt(path) {
      case S3LocationRx(protocol, bucketOrAccessPointAlias, relativePath) =>
        SimpleS3Location(protocol, bucketOrAccessPointAlias, relativePath)
    }

    def isValidS3Path: Boolean = S3LocationRx.pattern.matcher(path).matches

    def withoutTrailSlash: SimpleS3Location = {
      val parsedS3Location = SimpleS3Location(path)

      if (parsedS3Location.path.endsWith("/")) parsedS3Location.copy(path = parsedS3Location.path.dropRight(1))
      else parsedS3Location
    }

    def withTrailSlash: SimpleS3Location = {
      val parsedS3Location = SimpleS3Location(path)

      if (parsedS3Location.path.endsWith("/")) {
        parsedS3Location
      }
      else {
        val lastPartOfS3Path = parsedS3Location.path.split("/").last

        // If there is no dot in the path, then it still can be a file, but hopefully not vice versa,
        // i.e. if there is a dot then it shouldn't be a path. But strictly speaking, presence (or the lack) of the dot
        // character does not prove or disprove that a given location is a file or not.
        if (lastPartOfS3Path.contains("."))
          throw new IllegalArgumentException(
            s"Could not add '/' into S3 location because it contains file location: ${parsedS3Location.path}"
          )

        parsedS3Location.copy(path = s"${parsedS3Location.path}/")
      }
    }
  }
}

case class SimpleS3Location(protocol: String, bucketName: String, path: String) extends S3Location
