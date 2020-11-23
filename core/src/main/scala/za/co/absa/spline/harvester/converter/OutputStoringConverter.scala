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

package za.co.absa.spline.harvester.converter

import za.co.absa.commons.lang.Converter

import scala.collection.mutable

/**
 * converter that remembers all outputs
 */
trait OutputStoringConverter extends Converter {

  private val store = mutable.Buffer[To]()

  def values: Seq[To] = store

  abstract override def convert(arg: From): To = {
    val output = super.convert(arg)
    store += output
    output
  }
}
