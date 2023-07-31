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

package za.co.absa.commons.lang

import java.io._
import java.util.function.BiFunction
import java.util.{Properties, function}
import java.{util => ju}

object ImmutableProperties {

  def fromStream(stream: InputStream): ImmutableProperties = new ImmutableProperties(stream)

  def fromReader(reader: Reader): ImmutableProperties = new ImmutableProperties(reader)

  def empty: ImmutableProperties = new ImmutableProperties

  def apply(props: Properties): ImmutableProperties = {
    val caw = new CharArrayWriter
    props.store(caw, null)
    fromReader(new CharArrayReader(caw.toCharArray))
  }
}

class ImmutableProperties private(private[this] var isOngoingInit: Boolean = false) extends Properties {

  private def this(reader: Reader) = {
    this(isOngoingInit = true)
    super.load(reader)
    isOngoingInit = false
  }

  private def this(in: InputStream) = {
    this(isOngoingInit = true)
    super.load(in)
    isOngoingInit = false
  }

  /*
  * Properties methods
  */

  override def setProperty(key: String, value: String): Nothing = throw new UnsupportedOperationException

  override def load(reader: Reader): Nothing = throw new UnsupportedOperationException

  override def load(inStream: InputStream): Nothing = throw new UnsupportedOperationException

  override def loadFromXML(in: InputStream): Nothing = throw new UnsupportedOperationException

  override def stringPropertyNames(): ju.Set[String] = ju.Collections.unmodifiableSet(super.stringPropertyNames())

  /*
  * Hashtable methods
  */

  override def rehash(): Nothing = throw new UnsupportedOperationException

  override def put(key: AnyRef, value: AnyRef): AnyRef = {
    if (isOngoingInit) super.put(key, value) // called during initialization
    else throw new UnsupportedOperationException
  }

  override def remove(key: Any): Nothing = throw new UnsupportedOperationException

  override def putAll(t: ju.Map[_ <: AnyRef, _ <: AnyRef]): Nothing = throw new UnsupportedOperationException

  override def clear(): Nothing = throw new UnsupportedOperationException

  override def clone(): this.type = this

  override def keySet(): ju.Set[AnyRef] = ju.Collections.unmodifiableSet(super.keySet())

  override def entrySet(): ju.Set[ju.Map.Entry[AnyRef, AnyRef]] = ju.Collections.unmodifiableSet(super.entrySet())

  override def values(): ju.Collection[AnyRef] = ju.Collections.unmodifiableCollection(super.values())

  override def replaceAll(function: BiFunction[_ >: AnyRef, _ >: AnyRef, _ <: AnyRef]): Nothing = throw new UnsupportedOperationException

  override def putIfAbsent(key: AnyRef, value: AnyRef): Nothing = throw new UnsupportedOperationException

  override def remove(key: AnyRef, value: AnyRef): Nothing = throw new UnsupportedOperationException

  override def replace(key: AnyRef, oldValue: AnyRef, newValue: AnyRef): Nothing = throw new UnsupportedOperationException

  override def replace(key: AnyRef, value: AnyRef): Nothing = throw new UnsupportedOperationException

  override def merge(key: AnyRef, value: AnyRef, fn: BiFunction[_ >: AnyRef, _ >: AnyRef, _ <: AnyRef]): Nothing = throw new UnsupportedOperationException

  override def computeIfAbsent(key: AnyRef, fn: function.Function[_ >: AnyRef, _ <: AnyRef]): Nothing = throw new UnsupportedOperationException

  override def computeIfPresent(key: AnyRef, fn: BiFunction[_ >: AnyRef, _ >: AnyRef, _ <: AnyRef]): Nothing = throw new UnsupportedOperationException

  override def compute(key: AnyRef, fn: BiFunction[_ >: AnyRef, _ >: AnyRef, _ <: AnyRef]): Nothing = throw new UnsupportedOperationException
}
