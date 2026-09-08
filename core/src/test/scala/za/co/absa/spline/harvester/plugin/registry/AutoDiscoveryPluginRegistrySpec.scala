/*
 * Copyright 2026 ABSA Group Limited
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

package za.co.absa.spline.harvester.plugin.registry

import org.apache.commons.configuration.BaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.harvester.plugin.{Plugin, PluginsConfiguration}

class AutoDiscoveryPluginRegistrySpec extends AnyFlatSpec with Matchers {

  behavior of "AutoDiscoveryPluginRegistry"

  it should "skip a configured plugin that is not available on the classpath" in {
    val config = new BaseConfiguration
    config.setProperty("za.co.absa.spline.missing.Plugin.enabled", true)

    val registry = new AutoDiscoveryPluginRegistry(PluginsConfiguration(classpathScanEnabled = false, config))

    registry.plugins[Plugin] shouldBe empty
  }

  behavior of "tryLoadClass()"

  it should "return a loaded class" in {
    AutoDiscoveryPluginRegistry.tryLoadClass("java.lang.String", classOf[String]) shouldBe Some(classOf[String])
  }

  it should "return None when the class is missing" in {
    AutoDiscoveryPluginRegistry.tryLoadClass(
      "za.co.absa.spline.missing.Plugin",
      throw new ClassNotFoundException
    ) shouldBe None
  }

  it should "return None when a dependency is missing" in {
    AutoDiscoveryPluginRegistry.tryLoadClass(
      "za.co.absa.spline.ExistingPlugin",
      throw new NoClassDefFoundError
    ) shouldBe None
  }
}
