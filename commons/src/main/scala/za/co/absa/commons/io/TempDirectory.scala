/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.commons.io

import org.apache.commons.io.FileUtils

import java.io.IOException
import java.net.URI
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

/**
  * Represents a temporary directory
  *
  * @param prefix   name prefix
  * @param suffix   name suffix
  * @param pathOnly if <code>true</code>, no physical directory will be created on the file system
  */
class TempDirectory private(prefix: String, suffix: String, pathOnly: Boolean) {
  val path: Path = Files.createTempFile(prefix, suffix)
  Files.deleteIfExists(path)
  if (!pathOnly) Files.createDirectory(path)

  private lazy val hook = new Thread() {
    override def run(): Unit = delete()
  }

  /**
    * The directory will be recursively deleted on JVM shutdown.
    *
    * @return this [[TempDirectory]] instance
    */
  def deleteOnExit(): this.type = synchronized {
    Runtime.getRuntime.removeShutdownHook(hook)
    Runtime.getRuntime.addShutdownHook(hook)
    this
  }

  /**
    * Recursively delete directory.
    */
  def delete(): Unit = synchronized {
    if (Files.exists(path))
      Files.walkFileTree(path, new SimpleFileVisitor[Path] {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          FileUtils.deleteQuietly(file.toFile)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          FileUtils.deleteQuietly(dir.toFile)
          FileVisitResult.CONTINUE
        }
      })
  }

  /**
   * The directory path will converted to URI type.
   *
   * @return path as URI
   */
  def toURI: URI = path.toFile.toURI

  /**
   * The TempDirectory object will be converted to string representation which is stable and
   * equal on all platforms.
   *
   * @return string representation of current TempDirectory instance
   */
  def asString: String = path.toString.replace("\\", "/")
}

object TempDirectory {
  def apply(prefix: String = "", suffix: String = "", pathOnly: Boolean = false): TempDirectory =
    new TempDirectory(prefix, suffix, pathOnly)
}
