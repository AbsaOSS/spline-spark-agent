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
import za.co.absa.spline.harvester.plugin.Plugin
import za.co.absa.spline.harvester.plugin.Plugin.Precedence
import za.co.absa.spline.harvester.plugin.registry.AutoDiscoveryPluginRegistry.{EnabledByDefault, EnabledConfProperty, PluginClasses, getOnlyOrThrow}

import javax.annotation.Priority
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

class AutoDiscoveryPluginRegistry(
  conf: Configuration,
  injectables: AnyRef*
) extends PluginRegistry
  with Logging {

  private val injectablesByType: Map[Class[_], Seq[_ <: AnyRef]] = {
    val typedInjectables =
      for {
        o <- this +: injectables
        c = o.getClass
        t <- getAllSuperclasses(c).asScala ++ getAllInterfaces(c).asScala :+ c
      } yield t.asInstanceOf[Class[_]] -> o
    typedInjectables.groupBy(_._1).mapValues(_.map(_._2))
  }

  private val allPlugins: Seq[Plugin] =
    for (pc <- PluginClasses if isPluginEnabled(pc)) yield {
      logInfo(s"Loading plugin: $pc")
      instantiatePlugin(pc)
        .recover({ case NonFatal(e) => throw new RuntimeException(s"Plugin instantiation failure: $pc", e) })
        .get
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
        conf.subset(pluginClass.getName)
      case pt =>
        val candidates = injectablesByType.getOrElse(pt, sys.error(s"Cannot bind $pt. No value found"))
        getOnlyOrThrow(candidates, s"Ambiguous constructor parameter binding. Multiple values found for $pt: ${candidates.length}")
    }
    constr.newInstance(args: _*).asInstanceOf[Plugin]
  }

  private def isPluginEnabled(pc: Class[Plugin]): Boolean = {
    val pluginConf = conf.subset(pc.getName)
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

  private val PluginClasses: Seq[Class[Plugin]] = {
    logDebug("Scanning for plugins")
    val classGraph = new ClassGraph().enableClassInfo
    for {
      scanResult <- ARM.managed(classGraph.scan)
      (cls, prt) <- scanResult
        .getClassesImplementing(classOf[Plugin].getName)
        .loadClasses.asScala.asInstanceOf[Seq[Class[Plugin]]]
        .map(c => c -> priorityOf(c))
        .sortBy(_._2)
    } yield {
      logDebug(s"Found plugin [priority=$prt]\t: $cls")
      cls
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
