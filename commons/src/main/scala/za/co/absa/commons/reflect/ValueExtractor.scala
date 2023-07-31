/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.commons.reflect

import za.co.absa.commons.annotation.DeveloperApi
import za.co.absa.commons.reflect.ReflectionUtils.allInterfacesOf

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Mirror, runtimeMirror}

@DeveloperApi
class ValueExtractor[A: ClassTag, B](o: AnyRef, fieldName: String) {

  private val mirror: Mirror = runtimeMirror(getClass.getClassLoader)

  def extract(): B = {
    val declaringClass = implicitly[ClassTag[A]].runtimeClass
    reflectClassHierarchy(declaringClass)
      .orElse {
        // The field might be declared in a trait.
        reflectInterfaces(declaringClass)
      }
      .getOrElse(
        throw new NoSuchFieldException(s"${declaringClass.getName}.$fieldName")
      )
      .asInstanceOf[B]
  }

  @tailrec
  private def reflectClassHierarchy(c: Class[_]): Option[_] =
    if (c == classOf[AnyRef]) None
    else {
      val maybeValue: Option[Any] = reflectClass(c)
      if (maybeValue.isDefined) maybeValue
      else {
        val superClass = c.getSuperclass
        if (superClass == null) None
        else reflectClassHierarchy(superClass)
      }
    }

  private def reflectClass(c: Class[_]): Option[Any] =
    scalaReflectClass(c).orElse(javaReflectClass(c))

  /**
   *  may return None because:
   *  - `Symbols#CyclicReference` (Scala bug #12190)
   *  - `RuntimeException("scala signature")` (#80, #82)
   */
  private def scalaReflectClass(c: Class[_]): Option[Any] = util.Try {
    val members = mirror.classSymbol(c).toType.decls
    val m = members
      .filter(smb => (
        smb.toString.endsWith(s" $fieldName")
          && smb.isTerm
          && !smb.isConstructor
          && (!smb.isMethod || smb.asMethod.paramLists.forall(_.isEmpty))
        ))
      .minBy(!_.isMethod)

    val im = mirror.reflect(o)
    if (m.isMethod) im.reflectMethod(m.asMethod).apply()
    else im.reflectField(m.asTerm).get
  }.toOption

  private def javaReflectClass(c: Class[_]): Option[Any] =
    c.getDeclaredFields.collectFirst {
      case f if f.getName == fieldName =>
        f.setAccessible(true)
        f.get(o)
    } orElse {
      c.getDeclaredMethods.collectFirst {
        case m if m.getName == fieldName && m.getParameterCount == 0 =>
          m.setAccessible(true)
          m.invoke(o)
      }
    }


  private def reflectInterfaces(c: Class[_]) = {
    val altNames = allInterfacesOf(c)
      .map(_.getName.replace('.', '$') + "$$" + fieldName)

    c.getDeclaredFields.collectFirst {
      case f if altNames contains f.getName =>
        f.setAccessible(true)
        f.get(o)
    }
  }

}
