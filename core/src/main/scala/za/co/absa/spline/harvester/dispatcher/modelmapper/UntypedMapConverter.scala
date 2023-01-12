package za.co.absa.spline.harvester.dispatcher.modelmapper

import za.co.absa.spline.producer.model.AttrOrExprRef

class UntypedMapConverter(toAttrOrExprRef: (AttrOrExprRef => Any)) {

  def toUntypedMap(map: Map[String, Any]): Map[String, Any] = {
    def mapValue(value: Any): Any = value match {
      case aoe: AttrOrExprRef => toAttrOrExprRef(aoe)
      case s: Seq[_] => s.map(mapValue)
      case m: Map[_, _] => m.transform((k, v) => mapValue(v))
      case o: Option[_] => o.map(mapValue)
      case other => other
    }

    map.transform((k, v) => mapValue(v))
  }
}
