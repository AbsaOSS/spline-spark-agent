package za.co.absa.spline.harvester.logging

import java.lang.reflect.{Field, Method, Modifier}

import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue

import scala.collection.mutable

object ObjectStructureDumper {

  def dump(obj: Any): String = {
    val value = obj.asInstanceOf[AnyRef]

    val dumper = new innerDumper()
    val info = dumper.allFieldsToString(value, value.getClass.getDeclaredFields, value.getClass.getDeclaredMethods, 0)
    s"Data for instance of ${value.getClass}\n$info"
  }

  private class innerDumper() {
    val visited = mutable.Set[InstanceEqualityBox]()
    def addToVisited(obj: AnyRef): Unit = visited.add(InstanceEqualityBox(obj))
    def wasVisited(obj: AnyRef): Boolean = visited(InstanceEqualityBox(obj))

    case class InstanceEqualityBox(obj: AnyRef) {
      override def equals(otherObj: Any): Boolean = otherObj match {
        case InstanceEqualityBox(inner) => inner eq obj
        case _ => throw new UnsupportedOperationException()
      }

      override def hashCode(): Int = obj.getClass.getName.hashCode
    }

    def allFieldsToString(parent: AnyRef, fields: Seq[Field], methods: Seq[Method], depth: Int): String = {
      fields
        .filter(_.getName != "child")
        .filter(_.getName != "session")
        .filter(f => !Modifier.isStatic(f.getModifiers))
        .map { f =>
          val subValue = extractFieldValue[AnyRef](parent, f.getName)
          oneFieldToString(subValue, f, depth + 1)
        }
        .mkString("\n")
    }

    private def oneFieldToString(value: AnyRef, field: Field, depth: Int): String = {
      val fieldsDetails = value match {
        case _ if depth > 8 => "MAX DEPTH"
        case null => "= null"
        case v if isReadyForPrint(v) => s"= $v"
        case v if wasVisited(v) => "! Object was already logged"
        case None => "= None"
        case Some(x) => {
          addToVisited(value)
          s"Some\n${oneFieldToString(x.asInstanceOf[AnyRef], field, depth + 1)}"
        }
        case _ => {
          addToVisited(value)

          val details = allFieldsToString(value, value.getClass.getDeclaredFields, value.getClass.getDeclaredMethods, depth)

          if(details.nonEmpty)
            s"\n$details"
          else
            details
        }
      }

      val indent = " " * depth * 2
      s"$indent${field.getName}: ${field.getType.getName} $fieldsDetails"
    }
  }

  private def isReadyForPrint(value: AnyRef): Boolean = {
    isPrimitiveLike(value) ||
      Set("String")(value.getClass.getSimpleName) ||
      value.isInstanceOf[Traversable[_]] ||
      value.isInstanceOf[Enum[_]]
  }

  private def isPrimitiveLike(value: Any): Boolean = {
    val boxes = Set(
      classOf[java.lang.Boolean],
      classOf[java.lang.Byte],
      classOf[java.lang.Character],
      classOf[java.lang.Float],
      classOf[java.lang.Integer],
      classOf[java.lang.Long]
    ).map(_.getName)

    val isPrimitive = (value: Any) => value.getClass.isPrimitive
    val isBoxedPrimitive = (value: Any) => boxes(value.getClass.getName)

    isPrimitive(value) || isBoxedPrimitive(value)
  }
}
