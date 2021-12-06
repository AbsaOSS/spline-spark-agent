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

package za.co.absa.spline.agent

import org.apache.commons.configuration.{CompositeConfiguration, Configuration}
import za.co.absa.commons.HierarchicalObjectFactory
import za.co.absa.spline.agent.AgentConfig.ConfProperty
import za.co.absa.spline.harvester.IdGenerator.UUIDVersion
import za.co.absa.spline.harvester.conf.{SQLFailureCaptureMode, SplineMode}
import za.co.absa.spline.harvester.dispatcher.{CompositeLineageDispatcher, LineageDispatcher}
import za.co.absa.spline.harvester.iwd.IgnoredWriteDetectionStrategy
import za.co.absa.spline.harvester.postprocessing.{CompositePostProcessingFilter, PostProcessingFilter}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Try


private[spline] trait AgentBOM {
  def splineMode: SplineMode
  def sqlFailureCaptureMode: SQLFailureCaptureMode
  def postProcessingFilter: Option[PostProcessingFilter]
  def lineageDispatcher: LineageDispatcher
  def iwdStrategy: IgnoredWriteDetectionStrategy
  def execPlanUUIDVersion: UUIDVersion
}

object AgentBOM {

  import za.co.absa.commons.CollectionImplicits._
  import za.co.absa.commons.ConfigurationImplicits._
  import za.co.absa.commons.config.ConfigurationImplicits._

  def createFrom(configs: Seq[Configuration]): AgentBOM = new AgentBOM {
    private val mergedConfig = new CompositeConfiguration(configs.asJava)
    private val objectFactory = new HierarchicalObjectFactory(mergedConfig)

    override def splineMode: SplineMode = {
      mergedConfig.getRequiredEnum[SplineMode](ConfProperty.Mode)
    }

    override def sqlFailureCaptureMode: SQLFailureCaptureMode = {
      mergedConfig.getRequiredEnum[SQLFailureCaptureMode](ConfProperty.SQLFailureCaptureMode)
    }

    override def execPlanUUIDVersion: UUIDVersion = {
      mergedConfig.getRequiredInt(ConfProperty.ExecPlanUUIDVersion)
    }

    override lazy val postProcessingFilter: Option[PostProcessingFilter] = {
      tryObtainAll[PostProcessingFilter](ConfProperty.RootPostProcessingFilter)
        .map(new CompositePostProcessingFilter(_))
        .toOption
    }

    override lazy val lineageDispatcher: LineageDispatcher = {
      tryObtainAll[LineageDispatcher](ConfProperty.RootLineageDispatcher)
        .map(new CompositeLineageDispatcher(_, failOnErrors = false))
        .get
    }

    override lazy val iwdStrategy: IgnoredWriteDetectionStrategy = {
      obtainRequired[IgnoredWriteDetectionStrategy](ConfProperty.IgnoreWriteDetectionStrategy, mergedConfig)
    }

    private def obtainRequired[A <: AnyRef : ClassTag](key: String, conf: Configuration): A = {
      val value = conf.getRequiredObject[A](key)
      obtain[A](key, value)
    }

    private def tryObtainAll[A <: AnyRef : ClassTag](key: String): Try[Seq[A]] = {
      val triedRefs = configs.tryReduce[AnyRef](_.getRequiredObject[AnyRef](key))
      val triedVals = triedRefs.flatMap(_.tryReduce[A](obtain(key, _)))
      triedVals
    }

    private def obtain[A <: AnyRef : ClassTag](key: String, value: AnyRef): A = value match {
      case instance: A => instance
      case objName: String =>
        objectFactory
          .child(key)
          .child(objName)
          .instantiate[A]()
    }
  }
}
