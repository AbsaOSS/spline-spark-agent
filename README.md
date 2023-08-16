Spline Agent for Apache Spark&trade;
===

The _Spline Agent for Apache Spark&trade;_ is a complementary module to the [Spline project](https://absaoss.github.io/spline/)
that captures runtime lineage information from the Apache Spark jobs.

The agent is a Scala library that is embedded into the Spark driver, listening to Spark events, and capturing logical execution plans.
The collected metadata is then handed over to the lineage dispatcher, from where it can either be sent to the Spline server
(e.g. via REST API or Kafka), or used in another way, depending on selected dispatcher type (see [Lineage Dispatchers](#dispatchers)).

The agent can be used with or without a Spline server, depending on your use case. See [References](#references).

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa.spline.agent.spark/agent-core_2.12/badge.svg)](https://search.maven.org/search?q=g:za.co.absa.spline.agent.spark)
[![TeamCity build](https://teamcity.jetbrains.com/app/rest/builds/aggregated/strob:%28locator:%28buildType:%28id:OpenSourceProjects_AbsaOSS_SplineAgentSpark_AutoBuildSpark24scala212%29,branch:develop%29%29/statusIcon.svg)](https://teamcity.jetbrains.com/viewType.html?buildTypeId=OpenSourceProjects_AbsaOSS_SplineAgentSpark_AutoBuildSpark24scala212&branch=develop&tab=buildTypeStatusDiv)
[![Sonarcloud Status](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=alert_status)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![SonarCloud Maintainability](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![SonarCloud Reliability](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=reliability_rating)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![SonarCloud Security](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_spline-spark-agent&metric=security_rating)](https://sonarcloud.io/dashboard?id=AbsaOSS_spline-spark-agent)
[![Docker Pulls](https://badgen.net/docker/pulls/absaoss/spline-spark-agent?icon=docker&label=pulls)](https://hub.docker.com/r/absaoss/spline-spark-agent/)


## Table of Contents

<!--ts-->

* [Versioning](#versioning)
    * [Spark / Scala version compatibility matrix](#compat-matrix)
* [Usage](#usage)
    * [Selecting artifact](#selecting-artifact)
    * [Initialization](#initialization)
        * [Codeless](#initialization-codeless)
        * [Programmatic](#initialization-programmatic)
* [Configuration](#configuration)
    * [Properties](#properties)
    * [Lineage Dispatchers](#dispatchers)
    * [Post Processing Filters](#filters)
* [Spark features coverage](#spark-coverage)
* [Developer documentation](#dev-doc)
    * [Plugin API](#plugins)
    * [Building for different Scala and Spark versions](#building)
* [References and Examples](#references)

<!-- Added by: wajda, at: Fri 14 May 18:05:53 CEST 2021 -->

<!--te-->

<a id="versioning"></a>

## Versioning

The Spline Spark Agent follows the [Semantic Versioning](https://semver.org/) principles.
The _Public API_ is defined as a set of entry-point classes (`SparkLineageInitializer`, `SplineSparkSessionWrapper`),
extension APIs (Plugin API, filters, dispatchers), configuration properties and a set of supported Spark versions.
In other words, the _Spline Spark Agent Public API_ in terms of _SemVer_ covers all entities and abstractions that are designed
to be used or extended by client applications.

The version number **does not** directly reflect the relation of the Agent to the Spline Producer API (the Spline server). Both the Spline Server and
the Agent are designed to be as much mutually compatible as possible, assuming long-term operation and a possibly significant gap in the server and
the agent release dates. Such requirement is dictated by the nature of the Agent that could be embedded into some Spark jobs and only rarely if ever
updated without posing a risk to stop working because of eventual Spline server update. Likewise, it should be possible to update the Agent anytime
(e.g. to fix a bug or support a newer Spark version or a feature that earlier agent version didn't support) without requiring a Spline server upgrade.

Although not required by the above statement, for minimizing user astonishment when the compatibility between too distant _Agent_ and _Server_
versions is dropped, we'll increment the _Major_ version component.

<a id="compat-matrix"></a>

### Spark / Scala version compatibility matrix

|                        |         Scala 2.11         | Scala 2.12 |
|------------------------|:--------------------------:|:----------:|
| **Spark 2.2**          | (no SQL; no codeless init) |  &mdash;   |
| **Spark 2.3**          |     (no Delta support)     |  &mdash;   |
| **Spark 2.4**          |            Yes             |    Yes     |
| **Spark 3.0 or newer** |          &mdash;           |    Yes     |

<a id="usage"></a>

## Usage

<a id="selecting-artifact"></a>

### Selecting artifact

There are two main agent artifacts:

- `agent-core`
  is a Java library that you can use with any compatible Spark version. Use this one if you want to include Spline agent into your
  custom Spark application, and you want to manage all transitive dependencies yourself.

- `spark-spline-agent-bundle`
  is a fat jar that is designed to be embedded into the Spark driver, either by manually copying it to the Spark's `/jars` directory,
  or by using `--jars` or `--packages` argument for the `spark-submit`, `spark-shell` or `pyspark` commands.
  This artifact is self-sufficient and is **aimed to be used by most users**.

Because the bundle is pre-built with all necessary dependencies, it is important to select a proper version of it that matches the minor Spark
and Scala versions of your target Spark installation.

```
spark-A.B-spline-agent-bundle_X.Y.jar
```

here `A.B` is the first two Spark version numbers and `X.Y` is the first two Scala version numbers.
For example, if you have Spark 2.4.4 pre-built with Scala 2.12.10 then select the following agent bundle:

```
spark-2.4-spline-agent-bundle_2.12.jar
```

**AWS Glue Note**: dependency on `org.yaml:snakeyaml:1.33` is **missing** in Glue flavour of Spark. Please add this dependency on the classpath.

<a id="initialization"></a>

### Initialization

Spline agent is basically a Spark query listener that needs to be registered in a Spark session before is can be used.
Depending on if you are using it as a library in your custom Spark application, or as a standalone bundle you can choose
one of the following initialization approaches.

<a id="initialization-codeless"></a>

#### Codeless Initialization

This way is the most convenient one, can be used in majority use-cases.
Simply include the Spline listener into the `spark.sql.queryExecutionListeners` config property
(see [Static SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#static-sql-configuration))

Example:

```bash
pyspark \
  --packages za.co.absa.spline.agent.spark:spark-2.4-spline-agent-bundle_2.12:<VERSION> \
  --conf "spark.sql.queryExecutionListeners=za.co.absa.spline.harvester.listener.SplineQueryExecutionListener" \
  --conf "spark.spline.lineageDispatcher.http.producer.url=http://localhost:9090/producer"
```

The same approach works for `spark-submit` and `spark-shell` commands.

**Note**: all Spline properties set via Spark conf should be prefixed with `spark.` prefix in order to be visible to the Spline agent.  
See [Configuration](#configuration) section for details.

<a id="initialization-programmatic"></a>

#### Programmatic Initialization

**Note**: starting from Spline 0.6 most agent components can be configured or even replaced in a declarative manner
either using [Configuration](#configuration) or [Plugin API](#plugins). So normally there should be no need to use a programmatic initialization
method.
**We recommend to use [Codeless Initialization](#initialization-codeless) instead**.

But if for some reason, Codeless Initialization doesn't fit your needs, or you want to do more customization on Spark agent,
you can use programmatic initialization method.

```Scala
// given a Spark session ...
val sparkSession: SparkSession = ???

// ... enable data lineage tracking with Spline
import za.co.absa.spline.harvester.SparkLineageInitializer._
sparkSession.enableLineageTracking()

// ... then run some Dataset computations as usual.
// The lineage will be captured and sent to the configured Spline Producer endpoint.
```

or in Java syntax:

```java
import za.co.absa.spline.harvester.SparkLineageInitializer;
// ...
SparkLineageInitializer.enableLineageTracking(session);
```

The method `enableLineageTracking()` accepts optional `AgentConfig` object that can be used to customize Spline behavior.
This is an alternative way to configure Spline. The other one if via the [property based configuration](#configuration).

The instance of `AgentConfig` can be created by using a builder or one of the factory methods.

```scala
// from a sequence of key-value pairs 
val config = AgentConfig.from(???: Iterable[(String, Any)])

// from a Common Configuration
val config = AgentConfig.from(???: org.apache.commons.configuration.Configuration)

// using a builder
val config = AgentConfig.builder()
  // call some builder methods here...
  .build()

sparkSession.enableLineageTracking(config)
```

**Note**: `AgentConfig` object doesn't override the standard configuration stack. Instead, it serves as an additional configuration mean
with the precedence set between the `spline.yaml` and `spline.default.yaml` files (see below).

<a id="configuration"></a>

## Configuration

The agent looks for configuration in the following sources (listed in order of precedence):

- Hadoop configuration (`core-site.xml`)
- Spark configuration
- JVM system properties
- `spline.properties` file on classpath
- `spline.yaml` file on classpath
- `AgentConfig` object
- `spline.default.yaml` file on classpath

The file [spline.default.yaml](core/src/main/resources/spline.default.yaml) contains default values
for all Spline properties along with additional documentation.
It's a good idea to look in the file to see what properties are available.

The order of precedence might look counter-intuitive, as one would expect that explicitly provided config (`AgentConfig` instance) should
override ones defined in the outer scope. However, prioritizing global config to local one makes it easier to manage Spline settings centrally
on clusters, while still allowing room for customization by job developers.

For example, a company could require lineage metadata from jobs executed on a particular cluster to be sanitized, enhanced with some metrics
and credentials and stored in a certain metadata store (a database, file, Spline server etc). The Spline configuration needs to be set globally
and applied to all Spark jobs automatically. However, some jobs might contain hardcoded properties that the developers used locally or on
a testing environment, and forgot to remove them before submitting jobs into a production.
In such situation we want cluster settings to have precedence over the job settings.
Assuming that hardcoded settings would most likely be defined in the `AgentConfig` object, a property file or a JVM properties,
on the cluster we could define them in the Spark config or Hadoop config.

In case of multiple definitions of property the first occurrence wins, but `spline.lineageDispatcher` and `spline.postProcessingFilter` properties
are composed instead. E.g. if a _LineageDispatcher_ is set to be _Kafka_ in one config source and 'Http' in another, they would be implicitly
wrapped by a composite dispatcher, so both would be called in the order corresponding the config source precedence.
See `CompositeLineageDispatcher` and `CompositePostProcessingFilter`.

Every config property is resolved independently. So, for instance, if a `DataSourcePasswordReplacingFilter` is used some of its properties might be
taken from one config source and the other ones form another, according to the conflict resolution rules described above.
This allows administrators to tweak settings of individual Spline components (filters, dispatchers or plugins) without having to redefine and override
the whole piece of configuration for a given component.

<a id="properties"></a>

### Properties

#### `spline.mode`

- `ENABLED` [default]

  Spline will try to initialize itself, but if it fails it switches to DISABLED mode
  allowing the Spark application to proceed normally without Lineage tracking.

- `DISABLED`

  Lineage tracking is completely disabled and Spline is unhooked from Spark.

#### `spline.lineageDispatcher`

The logical name of the root lineage dispatcher. See [Lineage Dispatchers](#dispatchers) chapter.

#### `spline.postProcessingFilter`

The logical name of the root post-processing filter. See [Post Processing Filters](#filters) chapter.

<a id="dispatchers"></a>

### Lineage Dispatchers

The `LineageDispatcher` trait is responsible for sending out the captured lineage information.
By default, the `HttpLineageDispatcher` is used, that sends the lineage data to the Spline REST endpoint (see Spline Producer API).

Available dispatchers:

- `HttpLineageDispatcher` - sends lineage to an HTTP endpoint
- `KafkaLineageDispatcher` - sends lineage to a Kafka topic
- `ConsoleLineageDispatcher` - writes lineage to the console
- `LoggingLineageDispatcher` - logs lineage using the Spark logger
- `FallbackLineageDispatcher` - sends lineage to a fallback dispatcher if the primary one fails
- `CompositeLineageDispatcher` - allows to combine multiple dispatchers to send lineage to multiple endpoints

Each dispatcher can have different configuration parameters.
To make the configs clearly separated each dispatcher has its own namespace in which all it's parameters are defined.
I will explain it on a Kafka example.

Defining dispatcher

```properties
spline.lineageDispatcher=kafka
```

Once you defined the dispatcher all other parameters will have a namespace `spline.lineageDispatcher.{{dipatcher-name}}.` as a prefix.
In this case it is `spline.lineageDispatcher.kafka.`.

To find out which parameters you can use look into `spline.default.yaml`. For kafka I would have to define at least these two properties:

```properties
spline.lineageDispatcher.kafka.topic=foo
spline.lineageDispatcher.kafka.producer.bootstrap.servers=localhost:9092
```

#### Using the Http Dispatcher

This dispatcher is used by default. The only mandatory configuration is url of the producer API rest endpoint
(`spline.lineageDispatcher.http.producer.url`).
Additionally, timeouts, apiVersion and multiple custom headers can be set.

```properties
spline.lineageDispatcher.http.producer.url=
spline.lineageDispatcher.http.timeout.connection=2000
spline.lineageDispatcher.http.timeout.read=120000
spline.lineageDispatcher.http.apiVersion=LATEST
spline.lineageDispatcher.http.header.X-CUSTOM-HEADER=custom-header-value
```

If the producer requires token based authentication for requests, below mentioned details must be included in configuration.

```properties
spline.lineageDispatcher.http.authentication.type=OAUTH
spline.lineageDispatcher.http.authentication.grantType=client_credentials
spline.lineageDispatcher.http.authentication.clientId=<client_id>
spline.lineageDispatcher.http.authentication.clientSecret=<secret>
spline.lineageDispatcher.http.authentication.scope=<scope>
spline.lineageDispatcher.http.authentication.tokenUrl=<token_url>
```

Example: Azure HTTP trigger template API key header can be set like this:

```properties
spline.lineageDispatcher.http.header.X-FUNCTIONS-KEY=USER_API_KEY
```

Example: AWS Rest API key header can be set like this:

```properties
spline.lineageDispatcher.http.header.X-API-Key=USER_API_KEY
```

#### Using the Fallback Dispatcher

The `FallbackDispatcher` is a proxy dispatcher that sends lineage to the primary dispatcher first, and then _if_ there is an error
it calls the fallback one.

In the following example the `HttpLineageDispatcher` will be used as a primary, and the `ConsoleLineageDispatcher` as fallback.

```properties
spline.lineageDispatcher=fallback
spline.lineageDispatcher.fallback.primaryDispatcher=http
spline.lineageDispatcher.fallback.fallbackDispatcher=console
```

#### Using the Composite Dispatcher

The `CompositeDispatcher` is a proxy dispatcher that forwards lineage data to multiple dispatchers.

For example, if you want the lineage data to be sent to an HTTP endpoint and to be logged to the console at the same time you can do the following:

```properties
spline.lineageDispatcher=composite
spline.lineageDispatcher.composite.dispatchers=http,console
```

By default, if some dispatchers in the list fail, the others are still attempted. If you want the error in any dispatcher to be treated as fatal,
and be propagated to the main process, you set the `failOnErrors` property to `true`:

```properties
spline.lineageDispatcher.composite.failOnErrors=true
```

#### Creating your own dispatcher

There is also a possibility to create your own dispatcher. It must implement `LineageDispatcher` trait and have a constructor
with a single parameter of type `org.apache.commons.configuration.Configuration`.
To use it you must define name and class and also all other parameters you need. For example:

```properties
spline.lineageDispatcher=my-dispatcher
spline.lineageDispatcher.my-dispatcher.className=org.example.spline.MyDispatcherImpl
spline.lineageDispatcher.my-dispatcher.prop1=value1
spline.lineageDispatcher.my-dispatcher.prop2=value2
```

#### Combining dispatchers (complex example)

If you need, you can combine multiple dispatchers into a single one using `CompositeLineageDispatcher` and `FallbackLineageDispatcher`
in any combination as you wish.

In the following example the lineage will be first sent to the HTTP endpoint "http://10.20.111.222/lineage-primary", if that fails it's redirected to
the "http://10.20.111.222/lineage-secondary" endpoint, and if that one fails as well, lineage is logged to the ERROR logs and the console at the same
time.

```properties
spline.lineageDispatcher.http1.className=za.co.absa.spline.harvester.dispatcher.HttpLineageDispatcher
spline.lineageDispatcher.http1.producer.url=http://10.20.111.222/lineage-primary

spline.lineageDispatcher.http2.className=za.co.absa.spline.harvester.dispatcher.HttpLineageDispatcher
spline.lineageDispatcher.http2.producer.url=http://10.20.111.222/lineage-secondary

spline.lineageDispatcher.errorLogs.className=za.co.absa.spline.harvester.dispatcher.LoggingLineageDispatcher
spline.lineageDispatcher.errorLogs.level=ERROR

spline.lineageDispatcher.disp1.className=za.co.absa.spline.harvester.dispatcher.FallbackLineageDispatcher
spline.lineageDispatcher.disp1.primaryDispatcher=http1
spline.lineageDispatcher.disp1.fallbackDispatcher=disp2

spline.lineageDispatcher.disp2.className=za.co.absa.spline.harvester.dispatcher.FallbackLineageDispatcher
spline.lineageDispatcher.disp2.primaryDispatcher=http2
spline.lineageDispatcher.disp2.fallbackDispatcher=disp3

spline.lineageDispatcher.disp3.className=za.co.absa.spline.harvester.dispatcher.CompositeLineageDispatcher
spline.lineageDispatcher.composite.dispatchers=errorLogs,console

spline.lineageDispatcher=disp1
```

<a id="filters"></a>

### Post Processing Filters

Filters can be used to enrich the lineage with your own custom data or to remove unwanted data like passwords.
All filters are applied after the Spark plan is converted to Spline DTOs, but before the dispatcher is called.

The procedure how filters are registered and configured is similar to the `LineageDispatcher` registration and configuration procedure.
A custom filter class must implement `za.co.absa.spline.harvester.postprocessing.PostProcessingFilter` trait and declare a constructor
with a single parameter of type `org.apache.commons.configuration.Configuration`.
Then register and configure it like this:

```properties
spline.postProcessingFilter=my-filter
spline.postProcessingFilter.my-filter.className=my.awesome.CustomFilter
spline.postProcessingFilter.my-filter.prop1=value1
spline.postProcessingFilter.my-filter.prop2=value2
```

Use pre-registered `CompositePostProcessingFilter` to chain up multiple filters:

```properties
spline.postProcessingFilter=composite
spline.postProcessingFilter.composite.filters=myFilter1,myFilter2
```

(see `spline.default.yaml` for details and examples)

#### Using MetadataCollectingFilter

MetadataCollectingFilter provides a way to add additional data to lineage produced by Spline Agent.

Data can be added to the following lineage entities: `executionPlan`, `executionEvent`, `operation`, `read` and `write`.

Inside each entity is dedicated map named `extra` that can store any additional user data.

`executionPlan` and `executionEvent` have additional map `labels`. Labels are intended for identification and filtering on the server.

Example usage:

```properties
spline.postProcessingFilter=userExtraMeta
spline.postProcessingFilter.userExtraMeta.rules=file:///path/to/json-with-rules.json
```

json-with-rules.json could look like this:

```json
{
    "executionPlan": {
        "extra": {
            "my-extra-1": 42,
            "my-extra-2": [ "aaa", "bbb", "ccc" ]
        },
        "labels": {
            "my-label": "my-value"
        }
    },
    "write": {
        "extra": {
            "foo": "extra-value"
        }
    }
}
```

The `spline.postProcessingFilter.userExtraMeta.rules` can be either url pointing to json file or a json string.
The rules definition can be quite long and when providing string directly a lot of escaping may be necessary so using a file is recommended.

Example of escaping the rules string in Scala String:
```
.config("spline.postProcessingFilter.userExtraMeta.rules", "{\"executionPlan\":{\"extra\":{\"qux\":42\\,\"tags\":[\"aaa\"\\,\"bbb\"\\,\"ccc\"]}}}")
```
- `"` needs to be escaped because it would end the string
- `,` needs to be escaped because when passing configuration via Java properties the comma is used as a separator under the hood 
  and must be explicitly escaped.

Example of escaping the rules string as VM option:
```
-Dspline.postProcessingFilter.userExtraMeta.rules={\"executionPlan\":{\"extra\":{\"qux\":42\,\"tags\":[\"aaa\"\,\"bbb\"\,\"ccc\"]}}}
```

A convenient way how to provide rules json without need for escaping may be to specify the property in yaml config file.
An example of this can be seen in 
[spline examples yaml config](https://github.com/AbsaOSS/spline-spark-agent/blob/develop/examples/src/main/resources/spline.yaml).


There is also option to get environment variables using `$env`, jvm properties using `$jvm` and execute javascript using `$js`.
See the following example:

```json
{
    "executionPlan": {
        "extra": {
            "my-extra-1": 42,
            "my-extra-2": [ "aaa", "bbb", "ccc" ],
            "bar": { "$env": "BAR_HOME" },
            "baz": { "$jvm": "some.jvm.prop" },
            "daz": { "$js": "session.conf().get('k')" },
            "appName": { "$js":"session.sparkContext().appName()" }
       }
    }
}
```

For the javascript evaluation following variables are available by default:

| variable          | Scala Type                                                 |
|-------------------|:-----------------------------------------------------------|
| `session`         | `org.apache.spark.sql.SparkSession`                        |
| `logicalPlan`     | `org.apache.spark.sql.catalyst.plans.logical.LogicalPlan`  | 
| `executedPlanOpt` | `Option[org.apache.spark.sql.execution.SparkPlan]`         |

Using those objects it should be possible to extract almost any relevant information from Spark.

The rules can be conditional, meaning the specified params will be added only when some condition is met.
See the following example:

```json
{
    "executionEvent[@.timestamp > 65]": {
        "extra": { "tux": 1 }
    },
    "executionEvent[@.extra['foo'] == 'a' && @.extra['bar'] == 'x']": {
        "extra": { "bux": 2 }
    },
    "executionEvent[@.extra['foo'] == 'a' && !@.extra['bar']]": {
        "extra": { "dux": 3 }
    },
    "executionEvent[@.extra['baz'][2] >= 3]": {
        "extra": { "mux": 4 }
    },
    "executionEvent[@.extra['baz'][2] < 3]": {
        "extra": { "fux": 5 }
    },
    "executionEvent[session.sparkContext.conf['spark.ui.enabled'] == 'false']": {
      "extra": { "tux": 1 }
    }
}
```

The condition is enclosed by `[]` after entity name.
Here the `@` serves as a reference to currently processed entity, in this case executionEvent.
The `[]` inside the condition statement can also serve as a way to access maps and sequences.
Logical and comparison operators are available.

`session` and other variables available for js are available here as well.


For more examples of usage please se `MetadataCollectingFilterSpec` test class.

<a id="spark-coverage"></a>

## Spark features coverage

_Dataset_ operations are fully supported

_RDD_ transformations aren't supported due to Spark internal architecture specifics, but they might be supported semi-automatically
in the future Spline versions (see #33)

_SQL_ dialect is mostly supported.

_DDL_ operations are not supported, excepts for `CREATE TABLE ... AS SELECT ...` which is supported.

**Note**: By default, the lineage is only captured on persistent (write) actions.
To capture in-memory actions like `collect()`, `show()` etc the corresponding plugin needs to be activated
by setting up the following configuration property:

```properties
spline.plugins.za.co.absa.spline.harvester.plugin.embedded.NonPersistentActionsCapturePlugin.enabled=true
```

(See [spline.default.yaml](core/src/main/resources/spline.default.yaml#L230) for more information)

The following data formats and providers are supported out of the box:

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

Below is the break-down of the read/write command list that we have come through.  
Some commands are implemented, others have yet to be implemented,
and finally there are such that bear no lineage information and hence are ignored.

All commands inherit from `org.apache.spark.sql.catalyst.plans.logical.Command`.

You can see how to produce unimplemented commands in `za.co.absa.spline.harvester.SparkUnimplementedCommandsSpec`.

<a id="spark-coverage-done"></a>

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

<a id="spark-coverage-todo"></a>

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

When one of these commands occurs spline will let you know by logging a warning.

<a id="spark-coverage-ignored"></a>

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

<a id="dev-doc"></a>

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

- `BaseRelationProcessing` - similar to `ReadNodeProcessing`, but instead of capturing all logical plan nodes it only reacts on `LogicalRelation`
  (see `LogicalRelationPlugin`)
- `RelationProviderProcessing` - similar to `WriteNodeProcessing`, but it only captures `SaveIntoDataSourceCommand`
  (see `SaveIntoDataSourceCommandPlugin`)

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

**Note**: to avoid unwanted possible shadowing the other plugins (including the future ones),
make sure that the pattern-matching criteria are as much selective as possible for your plugin needs.

A plugin class is expected to only have a single constructor.
The constructor can have no arguments, or one or more of the following types (the values will be autowired):

- `SparkSession`
- `PathQualifier`
- `PluginRegistry`

Compile you plugin and drop it into the Spline/Spark classpath.
Spline will pick it up automatically.

<a id="building"></a>

### Building for different Scala and Spark versions

**Note:** The project requires Java version 1.8 (strictly) and [Apache Maven](https://maven.apache.org/) for building.

Check the build environment:

```shell
mvn --version
```

Verify that Maven is configured to run on Java 1.8. For example:

```
Apache Maven 3.6.3 (Red Hat 3.6.3-8)
Maven home: /usr/share/maven
Java version: 1.8.0_302, vendor: Red Hat, Inc., runtime: /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.302.b08-2.fc34.x86_64/jre
```

There are several maven profiles that makes it easy to build the project with different versions of Spark and Scala.

- Scala profiles: `scala-2.11`, `scala-2.12` (default)
- Spark profiles: `spark-2.2`, `spark-2.3`, `spark-2.4` (default), `spark-3.0`, `spark-3.1`, `spark-3.2`, `spark-3.3`

For example, to build an agent for Spark 2.4 and Scala 2.11:

```shell
# Change Scala version in pom.xml.
mvn scala-cross-build:change-version -Pscala-2.11

# now you can build for Scala 2.11
mvn clean install -Pscala-2.11,spark-2.4
```

### Build docker image

The agent docker image is mainly used to run [example jobs](examples/) and pre-fill the database with the sample lineage data.

(Spline docker images are available on the DockerHub repo - https://hub.docker.com/u/absaoss)

```shell
mvn install -Ddocker -Ddockerfile.repositoryUrl=my
```

See [How to build Spline Docker images](https://github.com/AbsaOSS/spline-getting-started/blob/main/building-docker.md) for details.

<a id="references"></a>

### How to measure code coverage
```shell
./mvn verify -Dcode-coverage
```
If module contains measurable data the code coverage report will be generated on path:
```
{local-path}\spline-spark-agent\{module}\target\site\jacoco
```

## References and examples

Although the primary goal of Spline agent is to be used in combination with the [Spline server](https://github.com/AbsaOSS/spline),
it is flexible enough to be used in isolation or integration with other data lineage tracking solutions including custom ones.

Below is a couple of examples of such integration:

- [Databricks Lineage In Azure Purview](https://intellishore.dk/data-lineage-from-databricks-to-azure-purview/)
- [Spark Compute Lineage to Datahub](https://firststr.com/2021/04/26/spark-compute-lineage-to-datahub)

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
