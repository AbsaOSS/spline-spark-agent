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

package za.co.absa.spline.harvester

import com.fasterxml.uuid.Generators
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._

import java.security.MessageDigest
import java.text.MessageFormat
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong


object IdGenerator {
  type UUIDVersion = Int
  type UUIDNamespace = UUID
  type UUIDGeneratorFactory[Seed, -Input <: AnyRef] = Seed => UUIDGenerator[Input]

  object UUIDGeneratorFactory {
    def forVersion[Seed, Input <: AnyRef](uuidVersion: UUIDVersion): UUIDGeneratorFactory[Seed, Input] = uuidVersion match {
      case 4 => _ => new UUID4IdGenerator
      case 3 => ns => new UUID3IdGenerator[Input](ns.asInstanceOf[UUIDNamespace])
      case 5 => ns => new UUID5IdGenerator[Input](ns.asInstanceOf[UUIDNamespace])
      case v => throw new IllegalArgumentException(s"UUID version $v is not supported")
    }
  }

  object UUIDNamespace {
    val DataType: UUID = UUID.fromString("1ec15c9d-73c0-42e5-872f-65d3feb849c4")
    val ExecutionPlan: UUID = UUID.fromString("475196d0-16ca-4cba-aec7-c9f2ddd9326c")
  }
}

trait UUIDGenerator[-A] {
  def nextId(entity: A): UUID
}

abstract class HashBasedUUIDGenerator[-A <: AnyRef](namespace: UUID, hashAlgorithm: String) extends UUIDGenerator[A] {
  private val digest = MessageDigest.getInstance(hashAlgorithm)
  private val generator = Generators.nameBasedGenerator(namespace, digest)

  override def nextId(entity: A): UUID = {
    val input = entity.toJson
    generator.generate(input)
  }
}

class UUID3IdGenerator[-A <: AnyRef](namespace: UUID) extends HashBasedUUIDGenerator[A](namespace, "MD5")

class UUID5IdGenerator[-A <: AnyRef](namespace: UUID) extends HashBasedUUIDGenerator[A](namespace, "SHA-1")

class UUID4IdGenerator extends UUIDGenerator[Any] {
  override def nextId(unused: Any): UUID = UUID.randomUUID()
}

class SequentialIdGenerator(pattern: String) {
  private val counter = new AtomicLong(0)

  def nextId(): String = {
    val i = counter.getAndIncrement()
    MessageFormat.format(pattern, Long.box(i))
  }
}

class DataTypeIdGenerator(numberTemplate: String, namespace: UUID) {
  val seqIdGen = new SequentialIdGenerator(numberTemplate)
  val UUIDGen = new UUID5IdGenerator[String](namespace)

  def nextId(): UUID = UUIDGen.nextId(seqIdGen.nextId())
}
