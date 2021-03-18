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

package za.co.absa.commons

import scala.util.matching.Regex

object S3Location {

  /**
   * Generally usable regex for validating S3 path, e.g. `s3://my-cool-bucket1/path/to/file/on/s3.txt`
   * Protocols `s3`, `s3n`, and `s3a` are allowed.
   * Bucket naming rules defined at [[https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules]] are instilled.
   */
  val S3LocationRx: Regex = "(s3[an]?)://([-a-z0-9.]{3,63})/(.*)".r

  implicit class StringS3LocationExt(path: String) {

    def toS3Location: Option[SimpleS3Location] = {
      path match {
        case S3LocationRx(protocol, bucketName, relativePath) => Some(SimpleS3Location(protocol, bucketName, relativePath))
        case _ => None
      }
    }

    def toS3LocationOrFail: SimpleS3Location = {
      this.toS3Location.getOrElse{
        throw new IllegalArgumentException(s"Could not parse S3 Location from $path using rx $S3LocationRx.")
      }
    }

    def isValidS3Path: Boolean = path match {
      case S3LocationRx(_, _, _) => true
      case _ => false
    }
  }
}

trait S3Location {
  def protocol: String
  def bucketName: String
  def path: String

  /**
   * Returns formatted S3 string, e.g. `s3://myBucket/path/to/somewhere`
   * @return formatted s3 string
   */
  def s3String: String = s"$protocol://$bucketName/$path"
}

case class SimpleS3Location(protocol: String, bucketName: String, path: String) extends S3Location
