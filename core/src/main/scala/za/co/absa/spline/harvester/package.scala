package za.co.absa.spline

import java.lang.reflect.InvocationTargetException

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

package object harvester extends Logging {

  def instantiate[T: ClassTag](configuration: Configuration, className: String): T = {
    val interfaceName: String = scala.reflect.classTag[T].runtimeClass.getSimpleName
    log debug s"Instantiating $interfaceName for class name: $className"
    try {
      Class.forName(className.trim)
        .getConstructor(classOf[Configuration])
        .newInstance(configuration)
        .asInstanceOf[T]
    }
    catch {
      case e: InvocationTargetException => throw e.getTargetException
    }
  }
}
