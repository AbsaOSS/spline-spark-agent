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

package za.co.absa.spline.harvester.conf

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import za.co.absa.commons.config.ConfigurationImplicits.ConfigurationRequiredWrapper
import za.co.absa.spline.harvester.conf.HierarchicalObjectFactory.ClassName

import java.lang.reflect.InvocationTargetException
import scala.reflect.ClassTag
import scala.util.Try

final class HierarchicalObjectFactory(
  val configuration: Configuration,
  val parent: HierarchicalObjectFactory = null
) extends Logging {

  def child(namespace: String): HierarchicalObjectFactory = {
    new HierarchicalObjectFactory(configuration.subset(namespace), this)
  }

  def instantiate[A: ClassTag](className: String = configuration.getRequiredString(ClassName)): A = {
    logDebug(s"Instantiating $className")
    try {
      val clazz = Class.forName(className.trim)
      Try(clazz.getConstructor(classOf[HierarchicalObjectFactory]).newInstance(this))
        .getOrElse(clazz.getConstructor(classOf[Configuration]).newInstance(configuration))
        .asInstanceOf[A]
    } catch {
      case e: InvocationTargetException => throw e.getTargetException
    }
  }
}

object HierarchicalObjectFactory {
  final val ClassName = "className"
}
