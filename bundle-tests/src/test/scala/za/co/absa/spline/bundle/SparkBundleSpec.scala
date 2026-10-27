/*
 * Copyright 2026 ABSA Group Limited
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

package za.co.absa.spline.bundle

import com.sun.net.httpserver.HttpServer
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetTable}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.shaded.jackson.databind.{JsonNode, ObjectMapper}
import za.co.absa.spline.commons.version.Version
import za.co.absa.spline.harvester.SparkLineageInitializer._
import za.co.absa.spline.harvester.dispatcher.KafkaLineageDispatcher
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener
import za.co.absa.spline.producer.model._

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.jar.JarFile
import scala.collection.JavaConverters._
import scala.io.Source
import scala.xml.{Node, XML}

class SparkBundleSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper

  test("load the listener and Kafka dispatcher from a self-contained shaded JAR") {
    val location = classOf[SplineQueryExecutionListener].getProtectionDomain.getCodeSource.getLocation
    location.getPath should endWith(".jar")
    classOf[KafkaLineageDispatcher].getProtectionDomain.getCodeSource.getLocation should be(location)
    classOf[StringSerializer].getProtectionDomain.getCodeSource.getLocation should be(location)

    val jar = new JarFile(new java.io.File(location.toURI))
    try {
      val entries = jar.entries.asScala.map(_.getName).toVector
      entries should contain("org/apache/kafka/clients/producer/KafkaProducer.class")
      entries should contain("za/co/absa/spline/shaded/org/yaml/snakeyaml/Yaml.class")
      entries.exists(_.startsWith("org/apache/spark/")) should be(false)
      entries.exists(_.startsWith("org/apache/hadoop/")) should be(false)
      entries.exists(_.startsWith("org/apache/commons/lang3/")) should be(false)
      entries.exists(_.startsWith("org/slf4j/")) should be(false)
      entries should not contain "scala/Predef.class"
      entries should not contain "scala/collection/immutable/List.class"
      entries should not contain "scala/Serializable.class"

      val classLoader = classOf[SplineQueryExecutionListener].getClassLoader
      entries.filter(entry => entry.startsWith("scala/") && entry.endsWith(".class")).foreach { entry =>
        val resources = classLoader.getResources(entry).asScala.toVector
        withClue(s"Bundled Scala-namespace class overlaps the Spark runtime: $entry: $resources: ") {
          resources should have size 1
        }
      }

      val sparkMinorVersion = SPARK_VERSION.split("\\.").take(2).mkString(".")
      val bundleDependencies = jarPomDependencies(jar, s"spark-$sparkMinorVersion-spline-agent-bundle_2.13")
      (bundleDependencies("kafka-clients") \ "optional").text should be("true")
      (bundleDependencies("scala-library") \ "scope").text should be("provided")
      (bundleDependencies("slf4j-api") \ "scope").text should be("provided")
      val coreDependencies = jarPomDependencies(jar, "agent-core_2.13")
      (coreDependencies("scala-library") \ "version").text should be("2.13.17")
      (coreDependencies("slf4j-api") \ "version").text should be("2.0.16")
      (coreDependencies("spark-sql_2.13") \ "version").text should startWith("4.0.")
      (coreDependencies("spark-sql_2.13") \ "scope").text should be("provided")
      (coreDependencies("graph-core_2.13") \ "version").text should be("1.13.5")
      (coreDependencies("scalaj-http_2.13") \ "version").text should be("2.4.2")
      coreDependencies should contain key "nashorn-core"
      (jarPomDependencies(jar, "agent-commons_2.13")("scala-library") \ "version").text should be("2.13.17")
    } finally jar.close()
  }

  test("dispatch plan and event JSON with Kafka headers and a common plan ID") {
    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    try {
      val dispatcher = new KafkaLineageDispatcher(Version.asSimple("1.2"), "lineage-test", producer)
      val id = java.util.UUID.randomUUID()
      val plan = ExecutionPlan(
        Some(id), "bundle-test", None, Map.empty,
        Operations(WriteOperation("file:/output", append = false, "op-0", "write", Seq.empty, Map.empty, Map.empty), Seq.empty, Seq.empty),
        Seq.empty, Expressions(Seq.empty, Seq.empty),
        NameAndVersion("Spark", SPARK_VERSION), NameAndVersion("Spline", "test"), Map.empty)
      val event = ExecutionEvent(id, Map.empty, System.currentTimeMillis(), Some(1L), None, None, Map.empty)

      dispatcher.send(plan)
      dispatcher.send(event)

      val records = producer.history().asScala.toVector
      records should have size 2
      records.map(_.key()) should be(Vector(id.toString, id.toString))
      records.foreach { record =>
        record.topic() should be("lineage-test")
        new String(record.headers().lastHeader("ABSA-Spline-API-Version").value(), UTF_8) should be("1.2")
      }
      records.zip(Seq("ExecutionPlan", "ExecutionEvent")).foreach { case (record, entityType) =>
        new String(record.headers().lastHeader("ABSA-Spline-Entity-Type").value(), UTF_8) should be(entityType)
        new String(record.headers().lastHeader("__TypeId__").value(), UTF_8) should be(s"${entityType}_1.2")
      }
      mapper.readTree(records.head.value()).path("id").asText() should be(id.toString)
      mapper.readTree(records.last.value()).path("planId").asText() should be(id.toString)
    } finally producer.close()
  }

  test("codeless initialization propagates to child sessions and captures V1 Parquet plan and event lineage") {
    withCapturedLineage(codeless = true) { (spark, requests, directory) =>
      val child = spark.newSession()
      child.enableLineageTracking()
      val input = directory.resolve("input")
      val output = directory.resolve("output")
      child.range(3).write.parquet(input.toString)
      val transformed = child.read.parquet(input.toString).selectExpr("id", "id + 1 AS next_id")
      transformed.queryExecution.analyzed.collect { case relation: LogicalRelation => relation } should have size 1
      transformed.write.parquet(output.toString)

      val (plan, event) = awaitLineage(requests, output)
      val reads = plan.path("operations").path("reads")
      reads.size() should be(1)
      new URI(reads.get(0).path("inputSources").get(0).asText()).getPath should be(input.toString)
      assertColumnDependencies(plan, Map("id" -> "id", "next_id" -> "id"))
      plan.path("expressions").path("functions").size() should be > 0
      plan.path("systemInfo").path("version").asText() should be(SPARK_VERSION)
      event.path("durationNs").asLong() should be > 0L
      event.has("error") should be(false)
      assertNoMoreLineage(requests)
    }
  }

  test("forced Parquet DSv2 captures V2 read/write nodes, column dependencies and a matching event") {
    withCapturedLineage(codeless = true) { (spark, requests, directory) =>
      val input = directory.resolve("input")
      val output = Files.createDirectories(directory.resolve("output"))
      spark.range(3).write.parquet(input.toString)
      val v1Sources = spark.conf.get("spark.sql.sources.useV1SourceList").split(",")
        .filterNot(_.trim.equalsIgnoreCase("parquet")).mkString(",")
      spark.conf.set("spark.sql.sources.useV1SourceList", v1Sources)
      val transformed = spark.read.parquet(input.toString).selectExpr("id", "id + 1 AS next_id")
      val options = new CaseInsensitiveStringMap(Map("path" -> output.toString).asJava)
      val table = new ParquetDataSourceV2().getTable(transformed.schema, Array.empty, options.asCaseSensitiveMap())
      table shouldBe a[ParquetTable]
      val target = DataSourceV2Relation.create(table, None, None, options)
      // DataFrameWriter deliberately falls back to V1 for file writes (SPARK-28396).
      // Execute the native V2 command instead of testing a V2 read followed by a V1 write.
      val execution = spark.sessionState.executePlan(AppendData.byName(target, transformed.queryExecution.analyzed, Map.empty))
      execution.assertCommandExecuted()
      val write = execution.analyzed match {
        case append: AppendData => append
        case other => fail(s"Expected a DSv2 AppendData write, got:\n${other.treeString}")
      }
      write.table shouldBe a[DataSourceV2Relation]
      val reads = write.query.collect { case relation: DataSourceV2Relation => relation }
      reads should have size 1
      reads.head.table shouldBe a[ParquetTable]

      val (plan, event) = awaitLineage(requests, output)
      val readOperations = plan.path("operations").path("reads")
      readOperations.size() should be(1)
      new URI(readOperations.get(0).path("inputSources").get(0).asText()).getPath should be(input.toString)
      plan.path("operations").path("write").path("append").asBoolean() should be(true)
      assertColumnDependencies(plan, Map("id" -> "id", "next_id" -> "id"))
      plan.path("expressions").path("functions").size() should be > 0
      event.path("planId").asText() should be(plan.path("id").asText())
      event.path("durationNs").asLong() should be > 0L
      event.has("error") should be(false)
      spark.read.parquet(output.toString).orderBy("id").collect().map(row => row.getLong(0) -> row.getLong(1)).toSeq should be(
        Seq(0L -> 1L, 1L -> 2L, 2L -> 3L))
    }
  }

  test("programmatic initialization is idempotent and captures SQL CTAS with an explicit provider") {
    withCapturedLineage(codeless = false) { (spark, requests, directory) =>
      spark.enableLineageTracking()
      spark.enableLineageTracking()
      val input = directory.resolve("input")
      val output = directory.resolve("table")
      spark.range(2).write.parquet(input.toString)
      spark.sql(s"CREATE TABLE bundle_output USING PARQUET LOCATION '${output.toUri}' AS SELECT id FROM parquet.`$input`")

      val (plan, event) = awaitLineage(requests, output)
      plan.path("operations").path("reads").size() should be(1)
      assertColumnDependencies(plan, Map("id" -> "id"))
      event.path("planId").asText() should be(plan.path("id").asText())
      spark.read.parquet(output.toString).count() should be(2L)
    }
  }

  private def withCapturedLineage(codeless: Boolean)(body: (SparkSession, LinkedBlockingQueue[JsonNode], Path) => Unit): Unit = {
    val requests = new LinkedBlockingQueue[JsonNode]
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/producer", exchange => {
      try {
        val input = Source.fromInputStream(exchange.getRequestBody, "UTF-8")
        val payload = try input.mkString finally input.close()
        requests.put(mapper.readTree(payload))
        exchange.sendResponseHeaders(201, -1)
      } finally exchange.close()
    })
    server.start()
    val directory = Files.createTempDirectory("spline-bundle-test")
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    var spark: SparkSession = null
    try {
      val builder = SparkSession.builder()
        .appName("spline-bundle-test")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toString)
        .config("spark.spline.lineageDispatcher", "http")
        .config("spark.spline.lineageDispatcher.http.producer.url", s"http://127.0.0.1:${server.getAddress.getPort}/producer")
        .config("spark.spline.lineageDispatcher.http.apiVersion", "1.2")
        .config("spark.spline.lineageDispatcher.http.requestCompression", "false")
      if (codeless) builder.config("spark.sql.queryExecutionListeners", classOf[SplineQueryExecutionListener].getName)
      spark = builder.getOrCreate()
      body(spark, requests, directory)
    } finally {
      try {
        if (spark != null) spark.stop()
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        server.stop(0)
        val paths = Files.walk(directory)
        try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
        finally paths.close()
      }
    }
  }

  private def awaitLineage(requests: LinkedBlockingQueue[JsonNode], output: Path): (JsonNode, JsonNode) = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    var plan: Option[JsonNode] = None
    var event: Option[JsonNode] = None
    while ((plan.isEmpty || event.isEmpty) && System.nanoTime() < deadline) {
      Option(requests.poll(1, TimeUnit.SECONDS)).foreach { payload =>
        if (payload.isObject && payload.has("operations")) {
          val uri = payload.path("operations").path("write").path("outputSource").asText()
          if (URI.create(uri).getPath.stripSuffix("/") == output.toString) plan = Some(payload)
        } else if (payload.isArray && plan.nonEmpty) {
          payload.elements().asScala.find(_.path("planId").asText() == plan.get.path("id").asText()).foreach(e => event = Some(e))
        }
      }
    }
    withClue(s"No plan/event pair was dispatched for $output: ") {
      plan should not be empty
      event should not be empty
    }
    plan.get -> event.get
  }

  private def assertNoMoreLineage(requests: LinkedBlockingQueue[JsonNode]): Unit =
    requests.poll(1, TimeUnit.SECONDS) should be(null)

  private def jarPomDependencies(jar: JarFile, artifactId: String): Map[String, Node] = {
    val entry = jar.getJarEntry(s"META-INF/maven/za.co.absa.spline.agent.spark/$artifactId/pom.xml")
    val input = jar.getInputStream(entry)
    val pom = try XML.load(input) finally input.close()
    (pom \ "dependencies" \ "dependency").map(dep => (dep \ "artifactId").text -> dep).toMap
  }

  private def assertColumnDependencies(plan: JsonNode, expected: Map[String, String]): Unit = {
    val operations = plan.path("operations")
    val reads = operations.path("reads").elements().asScala.toVector
    val byOperationId = (reads ++ operations.path("other").elements().asScala)
      .map(op => op.path("id").asText() -> op).toMap
    val attributes = plan.path("attributes").elements().asScala
      .map(attr => attr.path("id").asText() -> attr).toMap
    val expressions = (plan.path("expressions").path("functions").elements().asScala ++
      plan.path("expressions").path("constants").elements().asScala)
      .map(expr => expr.path("id").asText() -> expr).toMap
    val sourceIds = reads.flatMap(_.path("output").elements().asScala.map(_.asText())).toSet
    val sourceIdByName = sourceIds.map(id => attributes(id).path("name").asText() -> id).toMap

    def attributeSources(id: String): Set[String] =
      if (sourceIds.contains(id)) Set(id)
      else attributes(id).path("childRefs").elements().asScala.flatMap(referenceSources).toSet

    def referenceSources(ref: JsonNode): Set[String] = {
      assert(ref.hasNonNull("__attrId") ^ ref.hasNonNull("__exprId"), s"Invalid API 1.2 reference: $ref")
      if (ref.hasNonNull("__attrId")) attributeSources(ref.path("__attrId").asText())
      else expressions(ref.path("__exprId").asText()).path("childRefs").elements().asScala.flatMap(referenceSources).toSet
    }

    val outputIds = operations.path("write").path("childIds").elements().asScala
      .flatMap(id => byOperationId(id.asText()).path("output").elements().asScala.map(_.asText())).toVector
    val outputIdByName = outputIds.map(id => attributes(id).path("name").asText() -> id).toMap
    outputIdByName.keySet should be(expected.keySet)
    expected.foreach { case (outputName, sourceName) =>
      withClue(s"Column dependency for $outputName: ") {
        attributeSources(outputIdByName(outputName)) should be(Set(sourceIdByName(sourceName)))
      }
    }
  }
}
