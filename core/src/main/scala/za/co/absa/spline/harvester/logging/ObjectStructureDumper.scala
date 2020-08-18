package za.co.absa.spline.harvester.logging

import java.lang.reflect.Modifier

import za.co.absa.commons.reflect.ReflectionUtils.extractFieldValue

import scala.annotation.tailrec

object ObjectStructureDumper {

  def dump(obj: Any): String = {
    val value = obj.asInstanceOf[AnyRef]

    val initialValue = ObjectBox(value, "operation", value.getClass.getName, 0)

    val info = objectToStringRec(List(initialValue), Set.empty[InstanceEqualityBox], "")
    s"Data for instance of ${value.getClass}\n$info"
  }

  private case class InstanceEqualityBox(obj: AnyRef) {
    override def equals(otherObj: Any): Boolean = otherObj match {
      case InstanceEqualityBox(inner) => inner eq obj
      case _ => throw new UnsupportedOperationException()
    }

    override def hashCode(): Int = obj.getClass.getName.hashCode
  }

  private type VisitedSet = Set[InstanceEqualityBox]

  private def addToVisited(visited: VisitedSet, obj: AnyRef): VisitedSet = visited + InstanceEqualityBox(obj)

  private def wasVisited(visited: VisitedSet, obj: AnyRef): Boolean = visited(InstanceEqualityBox(obj))

  private case class ObjectBox(value: AnyRef, fieldName: String, fieldType: String, depth: Int)

  @tailrec
  private final def objectToStringRec(stack: List[ObjectBox], visited: VisitedSet, result: String): String = stack match {
    case Nil => result
    case head :: tail => {
      val value = head.value
      val depth = head.depth

      val (fieldsDetails, newStack, newVisited) = value match {
        case null => ("= null", tail, visited)
        case v if isReadyForPrint(v) => (s"= $v", tail, visited)
        case v if wasVisited(visited, v) => ("! Object was already logged", tail, visited)
        case None => ("= None", tail, visited)
        case Some(x) => {
          val newVal = ObjectBox(x.asInstanceOf[AnyRef], "x", x.getClass.getName, depth + 1)
          ("Some", newVal :: tail, addToVisited(visited, value))
        }
        case _ => {
          val newFields = value.getClass.getDeclaredFields
            .filter(f => !Set("child", "session")(f.getName))
            .filter(f => !Modifier.isStatic(f.getModifiers))
            .map { f =>
              val subValue = extractFieldValue[AnyRef](value, f.getName)
              ObjectBox(subValue, f.getName, f.getType.getName, depth + 1)
            }.toList

          ("", newFields ::: tail, addToVisited(visited, value))
        }
      }

      val indent = " " * depth * 2
      val line = s"$indent${head.fieldName}: ${head.fieldType} $fieldsDetails\n"

      objectToStringRec(newStack, newVisited, result + line)
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
