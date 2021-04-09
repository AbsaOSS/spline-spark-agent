Spark Agent / Harvester
===

This module is responsible for listening to spark command events and converting them to spline lineage.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa.spline.agent.spark/agent-core_2.12/badge.svg)](https://search.maven.org/search?q=g:za.co.absa.spline.agent.spark)
[![TeamCity build](https://teamcity.jetbrains.com/app/rest/builds/aggregated/strob:%28locator:%28buildType:%28id:OpenSourceProjects_AbsaOSS_SplineAgentSpark_AutoBuildSpark24scala212%29,branch:develop%29%29/statusIcon.svg)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_AbsaOSS_SplineAgentSpark_AutoBuildSpark24scala212&branch=develop&tab=buildTypeStatusDiv)
[![Sonarcloud Status](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=alert_status)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![SonarCloud Maintainability](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![SonarCloud Reliability](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![SonarCloud Security](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=security_rating)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)

## Spark / Scala version compatibility matrix

|            | Scala 2.11                   | Scala 2.12 |
|------------|:----------------------------:|:----------:|
|**Spark 2.2** | (no SQL; no codeless init) | &mdash;    |
|**Spark 2.3** | (no Delta support)         | &mdash;    |
|**Spark 2.4** | Yes                        | Yes        |
|**Spark 3.0** | &mdash;                    | Yes        |

## Artifacts
- `agent-core_Y` is a classic maven library that you can use with any compatible Spark version.
- `spark-X-spline-agent-bundle_Y` is a fat jar. That means it contains all dependencies inside.

X represents Spark version and Y represents Scala version.


## Spark commands support
The latest agent supports the following 
data formats and providers out of the box:
- Avro
- Cassandra
- COBOL
- Delta
- ElasticSearch
- Excel
- HDFS
- Hive
- JDBC
- Kafka
- MongoDB
- XML

Although Spark being an extensible piece of software can support much more,
it doesn't provide any universal API that Spline can utilize to capture
reads and write from/to everything that Spark supports.
Support for most of different data sources and formats has to be added to Spline one by one.
Fortunately starting with Spline 0.5.4 the auto discoverable [Plugin API](#plugins) 
has been introduced to make this process easier.

Below is the break down of the read/write command list that we have come through.  
Some commands are implemented, others have yet to be implemented, 
and finally there are such that bear no lineage information and hence are ignored.

All commands inherit from `org.apache.spark.sql.catalyst.plans.logical.Command`.

You can see how to produce unimplemented commands in `za.co.absa.spline.harvester.SparkUnimplementedCommandsSpec`.
### Implemented

- `CreateDataSourceTableAsSelectCommand`  (org.apache.spark.sql.execution.command)
- `CreateHiveTableAsSelectCommand`  (org.apache.spark.sql.hive.execution)
- `CreateTableCommand`  (org.apache.spark.sql.execution.command)
- `DropTableCommand`  (org.apache.spark.sql.execution.command)
- `InsertIntoDataSourceDirCommand`  (org.apache.spark.sql.execution.command)
- `InsertIntoHadoopFsRelationCommand`  (org.apache.spark.sql.execution.datasources)
- `InsertIntoHiveDirCommand`  (org.apache.spark.sql.hive.execution)
- `InsertIntoHiveTable`  (org.apache.spark.sql.hive.execution)
- `SaveIntoDataSourceCommand`  (org.apache.spark.sql.execution.datasources)

### To be implemented

- `AlterTableAddColumnsCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableChangeColumnCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableRenameCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableSetLocationCommand`  (org.apache.spark.sql.execution.command)
- `CreateDataSourceTableCommand`  (org.apache.spark.sql.execution.command)
- `CreateDatabaseCommand`  (org.apache.spark.sql.execution.command)
- `CreateTableLikeCommand`  (org.apache.spark.sql.execution.command)
- `DropDatabaseCommand`  (org.apache.spark.sql.execution.command)
- `LoadDataCommand`  (org.apache.spark.sql.execution.command)
- `TruncateTableCommand`  (org.apache.spark.sql.execution.command)

When one of these commands occurs spline will let you know. 
- When it's running in `REQUIRED` mode it will throw an `UnsupportedSparkCommandException`.
- When it's running in `BEST_EFFORT` mode it will just log a warning.
 
### Ignored

- `AddFileCommand`  (org.apache.spark.sql.execution.command)
- `AddJarCommand`  (org.apache.spark.sql.execution.command)
- `AlterDatabasePropertiesCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableAddPartitionCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableDropPartitionCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableRecoverPartitionsCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableRenamePartitionCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableSerDePropertiesCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableSetPropertiesCommand`  (org.apache.spark.sql.execution.command)
- `AlterTableUnsetPropertiesCommand`  (org.apache.spark.sql.execution.command)
- `AlterViewAsCommand`  (org.apache.spark.sql.execution.command)
- `AnalyzeColumnCommand`  (org.apache.spark.sql.execution.command)
- `AnalyzePartitionCommand`  (org.apache.spark.sql.execution.command)
- `AnalyzeTableCommand`  (org.apache.spark.sql.execution.command)
- `CacheTableCommand`  (org.apache.spark.sql.execution.command)
- `ClearCacheCommand`  (org.apache.spark.sql.execution.command)
- `CreateFunctionCommand`  (org.apache.spark.sql.execution.command)
- `CreateTempViewUsing`  (org.apache.spark.sql.execution.datasources)
- `CreateViewCommand`  (org.apache.spark.sql.execution.command)
- `DescribeColumnCommand`  (org.apache.spark.sql.execution.command)
- `DescribeDatabaseCommand`  (org.apache.spark.sql.execution.command)
- `DescribeFunctionCommand`  (org.apache.spark.sql.execution.command)
- `DescribeTableCommand`  (org.apache.spark.sql.execution.command)
- `DropFunctionCommand`  (org.apache.spark.sql.execution.command)
- `ExplainCommand`  (org.apache.spark.sql.execution.command)
- `InsertIntoDataSourceCommand`  (org.apache.spark.sql.execution.datasources) *
- `ListFilesCommand`  (org.apache.spark.sql.execution.command)
- `ListJarsCommand`  (org.apache.spark.sql.execution.command)
- `RefreshResource`  (org.apache.spark.sql.execution.datasources)
- `RefreshTable`  (org.apache.spark.sql.execution.datasources)
- `ResetCommand$` (org.apache.spark.sql.execution.command)
- `SetCommand`  (org.apache.spark.sql.execution.command)
- `SetDatabaseCommand`  (org.apache.spark.sql.execution.command)
- `ShowColumnsCommand`  (org.apache.spark.sql.execution.command)
- `ShowCreateTableCommand`  (org.apache.spark.sql.execution.command)
- `ShowDatabasesCommand`  (org.apache.spark.sql.execution.command)
- `ShowFunctionsCommand`  (org.apache.spark.sql.execution.command)
- `ShowPartitionsCommand`  (org.apache.spark.sql.execution.command)
- `ShowTablePropertiesCommand`  (org.apache.spark.sql.execution.command)
- `ShowTablesCommand`  (org.apache.spark.sql.execution.command)
- `StreamingExplainCommand`  (org.apache.spark.sql.execution.command)
- `UncacheTableCommand`  (org.apache.spark.sql.execution.command)


\*) `SaveIntoDataSourceCommand` is produced at the same time, and it's already implemented.


## Developer documentation

<a id="plugins"></a>
### Plugin API

Using a plugin API you can capture lineage from a 3rd party data source provider.
Spline discover plugins automatically by scanning a classpath, so no special steps required to register and configure a plugin.
All you need is to create a class extending the `za.co.absa.spline.harvester.plugin.Plugin` marker trait 
mixed with one or more `*Processing` traits, depending on your intention.

There are three general processing traits:
- `DataSourceFormatNameResolving` - returns a name of a data provider/format in use.
- `ReadNodeProcessing` - detects a read-command and gather meta information.
- `WriteNodeProcessing` - detects a write-command and gather meta information.


There are also two additional trait that handle common cases of reading and writing:
- `BaseRelationProcessing` - similar to `ReadNodeProcessing`, but instead of capturing all logical plan nodes it only reacts on `LogicalRelation` (see `LogicalRelationPlugin`)      
- `RelationProviderProcessing` - similar to `WriteNodeProcessing`, but it only captures `SaveIntoDataSourceCommand` (see `SaveIntoDataSourceCommandPlugin`) 

The best way to illustrate how plugins work is to look at the real working example,
e.g. [`za.co.absa.spline.harvester.plugin.embedded.JDBCPlugin`](core/src/main/scala/za/co/absa/spline/harvester/plugin/embedded/JDBCPlugin.scala)

The most common simplified pattern looks like this:
```scala
package my.spline.plugin

import javax.annotation.Priority
import za.co.absa.spline.harvester.builder._
import za.co.absa.spline.harvester.plugin.Plugin._
import za.co.absa.spline.harvester.plugin._

@Priority(Precedence.User) // not required, but can be used to control your plugin precedence in the plugin chain. Default value is `User`.  
class FooBarPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing {

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (FooBarRelation(a, b, c, d), lr) if /*more conditions*/ =>
      val dataFormat: Option[AnyRef] = ??? // data format being read (will be resolved by the `DataSourceFormatResolver` later)
      val dataSourceURI: String = ??? // a unique URI for the data source
      val params: Map[String, Any] = ??? // additional parameters characterizing the read-command. E.g. (connection protocol, access mode, driver options etc)

      (SourceIdentifier(dataFormat, dataSourceURI), params)
  }

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (provider, cmd) if provider == "foobar" || provider.isInstanceOf[FooBarProvider] =>
      val dataFormat: Option[AnyRef] = ??? // data format being written (will be resolved by the `DataSourceFormatResolver` later)
      val dataSourceURI: String = ??? // a unique URI for the data source
      val writeMode: SaveMode = ??? // was it Append or Overwrite?
      val query: LogicalPlan = ??? // the logical plan to get the rest of the lineage from
      val params: Map[String, Any] = ??? // additional parameters characterizing the write-command

      (SourceIdentifier(dataFormat, dataSourceURI), writeMode, query, params)
  }
}
```

**Please note**: to avoid unwanted possible shadowing the other plugins (including the future ones),
make sure that the pattern-matching criteria are as much selective as possible for your plugin needs. 

A plugin class is expected to only have a single constructor.
The constructor can have no arguments, or one or more of the following types (the values will be autowired):
- `SparkSession`  
- `PathQualifier`
- `PluginRegistry`

Compile you plugin and drop it into the Spline/Spark classpath.
Spline will pick it up automatically. 

### Building for different Scala and Spark versions
There are several maven profiles that makes it easy to build the project with different versions of Spark and Scala.
- Scala profiles: `scala-2.11`, `scala-2.12`
- Spark profiles: `spark-2.2`, `spark-2.3`, `spark-2.4`, `spark-3.0`

However, maven is not able to change an artifact name using profile. To do that we use `scala-cross-build-maven-plugin`.

Example of usage:
```
# Change Scala version in pom.xml.
mvn scala-cross-build:change-version -Pscala-2.12

# now you can build for Scala 2.12
mvn clean package -Pscala-2.12,spark-2.4

# Change back to the default Scala version.
mvn scala-cross-build:restore-version
```

---

    Copyright 2019 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
