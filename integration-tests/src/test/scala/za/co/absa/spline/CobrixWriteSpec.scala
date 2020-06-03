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


package za.co.absa.spline

import java.io.{BufferedWriter, FileOutputStream, FileWriter}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.commons.io.TempDirectory
import za.co.absa.commons.scalatest.ConditionalTestTags._
import za.co.absa.commons.version.Version._
import za.co.absa.spline.test.fixture.SparkFixture
import za.co.absa.spline.test.fixture.spline.SplineFixture

class CobrixWriteSpec extends AnyFlatSpec
  with Matchers
  with SparkFixture
  with SplineFixture {

  private val cobrixPath = TempDirectory(prefix = "cobrix", pathOnly = true).deleteOnExit().path.toFile.getAbsolutePath
  val copybook: String =
    """       01  RECORD.
      |           05  ID                        PIC S9(4)  COMP.
      |           05  COMPANY.
      |               10  SHORT-NAME            PIC X(10).
      |               10  COMPANY-ID-NUM        PIC 9(5) COMP-3.
      |               10  COMPANY-ID-STR
      |			         REDEFINES  COMPANY-ID-NUM PIC X(3).
      |           05  METADATA.
      |               10  CLIENTID              PIC X(15).
      |               10  REGISTRATION-NUM      PIC X(10).
      |               10  NUMBER-OF-ACCTS       PIC 9(03) COMP-3.
      |               10  ACCOUNT.
      |                   12  ACCOUNT-DETAIL    OCCURS 80
      |                                         DEPENDING ON NUMBER-OF-ACCTS.
      |                      15  ACCOUNT-NUMBER     PIC X(24).
      |                      15  ACCOUNT-TYPE-N     PIC 9(5) COMP-3.
      |                      15  ACCOUNT-TYPE-X     REDEFINES
      |                           ACCOUNT-TYPE-N  PIC X(3).
      |
      |""".stripMargin

  val bytes: Array[Byte]  = Array[Byte](
    0x00.toByte, 0x06.toByte, 0xC5.toByte, 0xE7.toByte, 0xC1.toByte, 0xD4.toByte, 0xD7.toByte, 0xD3.toByte,
    0xC5.toByte, 0xF4.toByte, 0x40.toByte, 0x40.toByte, 0x00.toByte, 0x00.toByte, 0x0F.toByte, 0x40.toByte,
    0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte,
    0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte,
    0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte, 0x40.toByte,
    0x00.toByte, 0x3F.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0xF2.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF4.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0xF1.toByte, 0xF2.toByte, 0x00.toByte, 0x00.toByte, 0x0F.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF3.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF4.toByte,
    0xF0.toByte, 0xF0.toByte, 0xF1.toByte, 0xF0.toByte, 0xF2.toByte, 0x00.toByte, 0x00.toByte, 0x1F.toByte,
    0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0xF5.toByte, 0xF0.toByte, 0xF0.toByte, 0xF6.toByte, 0xF0.toByte, 0xF0.toByte, 0xF1.toByte, 0xF2.toByte,
    0xF0.toByte, 0xF0.toByte, 0xF3.toByte, 0xF0.toByte, 0xF1.toByte, 0xF0.toByte, 0xF0.toByte, 0xF0.toByte,
    0x00.toByte, 0x00.toByte, 0x2F.toByte
  )

  val bos = new FileOutputStream(cobrixPath)
  bos.write(bytes)
  bos.close()

  it should "support Cobrix as a source" in
    withNewSparkSession(spark => {
      withLineageTracking(spark)(lineageCaptor => {

        val (plan1, _) = lineageCaptor.lineageOf(spark
          .read
          .format("cobol")
          .option("copybook_contents", copybook)
          .option("is_record_sequence", "true")
          .option("schema_retention_policy", "collapse_root")
          .option("segment_id_level0", "R")
          .option("segment_id_prefix", "ID")
          .load(cobrixPath).write.mode(Overwrite).saveAsTable("somewhere")
        )
        plan1.operations.write.append shouldBe false
        plan1.operations.reads.get.head.inputSources.head shouldBe cobrixPath
        plan1.operations.reads.get.head.extra.get("sourceType") shouldBe Some("cobrix")



      })
    })
}
