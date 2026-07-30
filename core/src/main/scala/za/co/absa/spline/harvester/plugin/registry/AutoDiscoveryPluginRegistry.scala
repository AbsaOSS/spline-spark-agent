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

package za.co.absa.spline.harvester.plugin.registry

import io.github.classgraph.ClassGraph
import org.apache.commons.configuration.Configuration
import org.apache.commons.lang.ClassUtils.{getAllInterfaces, getAllSuperclasses}
import org.apache.spark.internal.Logging
import za.co.absa.spline.commons.lang.ARM
import za.co.absa.spline.harvester.plugin.Plugin.Precedence
import za.co.absa.spline.harvester.plugin.{Plugin, PluginsConfiguration}

import javax.annotation.Priority
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

class AutoDiscoveryPluginRegistry(
  pluginsConf: PluginsConfiguration,
  injectables: AnyRef*
) extends PluginRegistry
  with Logging {

  import za.co.absa.spline.harvester.plugin.registry.AutoDiscoveryPluginRegistry._

  private val injectablesByType: Map[Class[_], Seq[_ <: AnyRef]] = {
    val typedInjectables =
      for {
        o <- this +: injectables
        c = o.getClass
        t <- getAllSuperclasses(c).asScala ++ getAllInterfaces(c).asScala :+ c
      } yield t.asInstanceOf[Class[_]] -> o
    typedInjectables.groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }.toMap
  }

  private val allPlugins: Seq[Plugin] = {
    val discoveredClasses: Seq[Class[Plugin]] =
      if (pluginsConf.classpathScanEnabled) scanForPluginClasses()
      else {
        logInfo(s"Classpath scanning is DISABLED. Only explicitly configured plugins will be loaded.")
        Seq.empty
      }

    val configuredClasses: Seq[Class[Plugin]] = getRegisteredPluginClasses(pluginsConf.config)

    val allFoundPluginClasses: Seq[Class[Plugin]] = (discoveredClasses ++ configuredClasses).distinct

    val allSortedPluginClasses = allFoundPluginClasses
      .map(c => c -> priorityOf(c))
      .sortBy({ case (_, p) => p })
      .map({ case (c, _) => c })

    for (cls <- allSortedPluginClasses if isPluginEnabled(cls)) yield {
      logInfo(s"Loading plugin: $cls")
      instantiatePlugin(cls)
        .recover({ case NonFatal(e) => throw new RuntimeException(s"Plugin instantiation failure: $cls", e) })
        .get
    }
  }

  override def plugins[A: ClassTag]: Seq[Plugin with A] = {
    val ct = implicitly[ClassTag[A]]
    allPlugins.collect({ case p: Plugin if ct.runtimeClass.isInstance(p) => p.asInstanceOf[Plugin with A] })
  }

  private def instantiatePlugin(pluginClass: Class[_]): Try[Plugin] = Try {
    val constrs = pluginClass.getConstructors
    val constr = getOnlyOrThrow(constrs, s"Plugin class must have a single public constructor: ${constrs.mkString(", ")}")
    val args = constr.getParameterTypes.map {
      case ct if classOf[Configuration].isAssignableFrom(ct) =>
        pluginsConf.config.subset(pluginClass.getName)
      case pt =>
        val candidates = injectablesByType.getOrElse(pt, sys.error(s"Cannot bind $pt. No value found"))
        getOnlyOrThrow(candidates, s"Ambiguous constructor parameter binding. Multiple values found for $pt: ${candidates.length}")
    }
    constr.newInstance(args: _*).asInstanceOf[Plugin]
  }

  private def isPluginEnabled(pc: Class[Plugin]): Boolean = {
    val pluginConf = pluginsConf.config.subset(pc.getName)
    val isEnabled = pluginConf.getBoolean(EnabledConfProperty, EnabledByDefault)
    if (!isEnabled) {
      logWarning(s"Plugin ${pc.getName} is disabled in the configuration.")
    }
    isEnabled
  }

}

object AutoDiscoveryPluginRegistry extends Logging {

  private val EnabledConfProperty = "enabled"
  private val EnabledByDefault = true

  private def scanForPluginClasses(): Seq[Class[Plugin]] = {
    logDebug("Scanning for plugins")
    val classGraph = new ClassGraph().enableClassInfo
    for {
      scanResult <- ARM.managed(classGraph.scan)
      cls <- scanResult
        .getClassesImplementing(classOf[Plugin].getName)
        .loadClasses.asScala.toSeq.asInstanceOf[Seq[Class[Plugin]]]
    } yield {
      logDebug(s"Discovered plugin: $cls")
      cls
    }
  }

  private def getRegisteredPluginClasses(conf: Configuration): Seq[Class[Plugin]] = {
    for {
      key <- conf.getKeys.asScala.toSeq
      if key.endsWith(s".$EnabledConfProperty") // Looking for keys ending with ".enabled", since plugins must be explicitly enabled
      className = key.dropRight(EnabledConfProperty.length + 1) // Dropping ".enabled" to get plugin class name
      // a configured plugin, or a library it depends on, may be absent from the classpath
      // of a particular build, e.g. MongoPlugin that isn't compiled for Scala 2.13
      cls <- try Some(Class.forName(className)) catch {
        case _: NoClassDefFoundError | _: ClassNotFoundException =>
          logWarning(s"Configured plugin class is not available, skipping: $className")
          None
      }
      if classOf[Plugin].isAssignableFrom(cls)
    } yield {
      logDebug(s"Found registered plugin: $cls")
      cls.asInstanceOf[Class[Plugin]]
    }
  }

  private def priorityOf(c: Class[Plugin]): Int =
    Option(c.getAnnotation(classOf[Priority]))
      .map(_.value)
      .getOrElse(Precedence.User)

  private def getOnlyOrThrow[A](xs: Seq[A], msg: => String): A = xs match {
    case Seq(x) => x
    case _ => sys.error(msg)
  }
}
