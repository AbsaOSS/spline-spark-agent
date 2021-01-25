/*
 * Copyright 2019 ABSA Group Limited
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

import za.co.absa.commons.lang.CachingConverter
import za.co.absa.spline.harvester.converter._

import java.util.concurrent.atomic.AtomicInteger

class ComponentCreatorFactory {
  val dataConverter = new DataConverter
  val dataTypeConverter = new DataTypeConverter with CachingConverter

  private[this] val lastId = new AtomicInteger(0)

  def nextId: Int = lastId.getAndIncrement()
}
