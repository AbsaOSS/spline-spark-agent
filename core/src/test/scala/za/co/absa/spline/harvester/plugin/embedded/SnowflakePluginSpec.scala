/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.spline.harvester.plugin.embedded

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import za.co.absa.spline.harvester.plugin.Plugin.{ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.builder.SourceIdentifier
import org.mockito.Mockito.{mock, _}
import net.snowflake.spark.snowflake.Parameters
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.commons.reflect.{ReflectionUtils, ValueExtractor}

class SnowflakePluginSpec extends AnyFlatSpec with Matchers with MockitoSugar {
  "SnowflakePlugin" should "process Snowflake relation providers" in {
    // Setup
    val spark = mock[SparkSession]
    val plugin = new SnowflakePlugin(spark)

    val options = Map(
      "sfUrl" -> "test-url",
      "sfWarehouse" -> "test-warehouse",
      "sfDatabase" -> "test-database",
      "sfSchema" -> "test-schema",
      "sfUser" -> "user1",
      "dbtable" -> "test-table"
    )

    val cmd = mock[SaveIntoDataSourceCommand]
    when(cmd.options) thenReturn(options)
    when(cmd.mode) thenReturn(SaveMode.Overwrite)
    when(cmd.query) thenReturn(null)

    // Mocking the relation provider to be Snowflake
    val snowflakeRP = "net.snowflake.spark.snowflake.DefaultSource"

    // Execute
    val result = plugin.relationProviderProcessor((snowflakeRP, cmd))

    // Verify
    val expectedSourceId = SourceIdentifier(Some("snowflake"), "snowflake://test-url.test-warehouse.test-database.test-schema.test-table")
    result shouldEqual WriteNodeInfo(expectedSourceId, SaveMode.Overwrite, null, options)
  }

  it should "not process non-Snowflake relation providers" in {
    // Setup
    val spark = mock[SparkSession]
    val plugin = new SnowflakePlugin(spark)

    val cmd = mock[SaveIntoDataSourceCommand]

    // Mocking the relation provider to be non-Snowflake
    val nonSnowflakeRP = "some.other.datasource"

    // Execute & Verify
    assertThrows[MatchError] {
      plugin.relationProviderProcessor((nonSnowflakeRP, cmd))
    }
  }
}
