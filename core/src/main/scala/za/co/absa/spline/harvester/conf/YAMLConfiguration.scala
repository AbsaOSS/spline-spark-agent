/*
 * Copyright 2017 ABSA Group Limited
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

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils.isBlank
import org.yaml.snakeyaml.Yaml
import za.co.absa.spline.commons.lang.ARM

import java.net.URL
import java.{util => ju}
import scala.collection.JavaConverters._


/**
 * A configuration based on YAML 1.1 config file
 *
 * @param yaml yaml config as string
 */
class YAMLConfiguration(yaml: String)
  extends ReadOnlyConfiguration {

  def this(url: URL) = this(ARM.using(url.openStream())(IOUtils.toString))

  private val confMap: ju.Map[String, _ <: AnyRef] = {
    if (isBlank(yaml)) ju.Collections.emptyMap()
    else {
      val yamlMap = (new Yaml).load[ju.Map[String, AnyRef]](yaml)
      populateRecursively("", yamlMap, new ju.HashMap[String, AnyRef]())
    }
  }

  private def populateRecursively(prefix: String, srcMap: ju.Map[String, AnyRef], dstMap: ju.Map[String, AnyRef]): ju.Map[String, AnyRef] = {
    srcMap.entrySet.asScala.foreach((entry: ju.Map.Entry[String, AnyRef]) => {
      val srcKeyOpt = Option(entry.getKey)
      val dstKey = srcKeyOpt
        .map(srcKey => {
          if (prefix.isEmpty) srcKey
          else s"$prefix.$srcKey"
        })
        .getOrElse(prefix)

      entry.getValue match {
        case null => // ignore entries with `null` value
        case m: ju.Map[_, _] =>
          populateRecursively(dstKey, m.asInstanceOf[ju.Map[String, AnyRef]], dstMap)
        case xs: ju.List[_] =>
          dstMap.put(dstKey, xs)
        case v =>
          dstMap.put(dstKey, v.toString)
      }
    })
    dstMap
  }

  override def getProperty(key: String): AnyRef = confMap.get(key)

  override def getKeys: ju.Iterator[String] = confMap.keySet.iterator

  override def containsKey(key: String): Boolean = confMap.containsKey(key)

  override def isEmpty: Boolean = confMap.isEmpty
}
