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

package za.co.absa.spline.harvester.plugin.embedded

import org.apache.commons.codec.binary.Hex
import org.apache.commons.configuration.Configuration
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import za.co.absa.spline.agent.SplineAgent.FuncName
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.ModelConstants.CommonExtras
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.NonPersistentActionsCapturePlugin._
import za.co.absa.spline.harvester.plugin.{Plugin, WriteNodeProcessing}

import java.net.{InetAddress, NetworkInterface}
import java.util.UUID
import javax.annotation.Priority
import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

@Priority(Precedence.Normal)
class NonPersistentActionsCapturePlugin(
  conf: Configuration)
  extends Plugin
    with WriteNodeProcessing {

  private val actionNames: Set[String] = conf.getRequiredStringArray(ConfProps.FuncNames).toSet

  override val writeNodeProcessor: PartialFunction[(FuncName, LogicalPlan), WriteNodeInfo] = {
    case (funcName, lp: LogicalPlan) if actionNames(funcName) =>
      WriteNodeInfo(
        SourceIdentifier(None, s"memory://$LocalMacAddressString/$JVMId"),
        SaveMode.Overwrite,
        lp,
        name = funcName,
        extras = Map(CommonExtras.Synthetic -> true)
      )
  }
}

object NonPersistentActionsCapturePlugin {

  private object ConfProps {
    val FuncNames = "funcNames"
  }

  private val JVMId = s"jvm_${UUID.randomUUID()}"
  private val NullMacAddress = Array.fill[Byte](6)(0)

  private val LocalMacAddressString: String = {
    val localAddress = InetAddress.getLocalHost
    val hardwareAddress =
      Option(NetworkInterface.getByInetAddress(localAddress))
        .flatMap(iface => Option(iface.getHardwareAddress))
        .getOrElse {
          // Local address was resolved incorrectly (see issue #634)
          // Let's try to find the most suitable interface by ourselves
          val ifaces = NetworkInterface.getNetworkInterfaces.asScala.toSeq
            .filter(iface => !iface.isVirtual && !iface.isLoopback && iface.getHardwareAddress != null)
            .sortBy(iface => !iface.getInetAddresses.asScala.exists(_.isSiteLocalAddress))
          ifaces match {
            case iface +: _ => iface.getHardwareAddress
            case _ => NullMacAddress
          }
        }
    Hex.encodeHexString(hardwareAddress).grouped(2).toArray.mkString("-")
  }
}
