/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.plugin

import io.github.classgraph.ClassGraph
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.commons.lang.ARM
import za.co.absa.spline.harvester.qualifier.PathQualifier

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

trait PluginRegistry {
  def plugins: Seq[Plugin]
}

class PluginRegistryImpl(pathQualifier: PathQualifier, session: SparkSession)
  extends PluginRegistry
    with Logging {

  private val pluginArgsByType: Map[Class[_], AnyRef] = Map(
    classOf[PluginRegistry] -> this,
    classOf[SparkSession] -> session,
    classOf[PathQualifier] -> pathQualifier
  )

  override val plugins: Seq[Plugin] = {
    log.info("Scanning for plugins")
    val classGraph = new ClassGraph()
      .enableClassInfo
      .enableAnnotationInfo
    for {
      scanResult <- ARM.managed(classGraph.scan)
      pluginClass <- scanResult.getClassesImplementing(classOf[Plugin].getName).loadClasses.asScala
    } yield {
      log.info(s"Loading plugin: $pluginClass")
      instantiatePlugin(pluginClass)
        .recover({
          case NonFatal(e) => throw new RuntimeException(s"Plugin instantiation failure: $pluginClass", e)
        })
        .get
    }
  }

  private def instantiatePlugin(pluginClass: Class[_]): Try[Plugin] = Try {
    val constructors = pluginClass.getConstructors
    if (constructors.length != 1)
      sys.error(s"Cannot instantiate a plugin with multiple constructors: $pluginClass")
    val constr = constructors.head
    val args = constr.getParameterTypes.map(pt => pluginArgsByType.getOrElse(pt, sys.error(s"Cannot find value for $pt")))
    constr.newInstance(args: _*).asInstanceOf[Plugin]
  }
}
