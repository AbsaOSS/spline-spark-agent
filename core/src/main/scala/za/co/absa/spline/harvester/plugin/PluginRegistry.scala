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

package za.co.absa.spline.harvester.plugin

import org.apache.spark.sql.SparkSession
import za.co.absa.spline.harvester.plugin.composite.{LogicalRelationPlugin, SaveIntoDataSourceCommandPlugin}
import za.co.absa.spline.harvester.plugin.impl._
import za.co.absa.spline.harvester.qualifier.PathQualifier

trait PluginRegistry {
  def plugins: Seq[Plugin]
}

class PluginRegistryImpl(pathQualifier: PathQualifier, session: SparkSession) extends PluginRegistry {
  override val plugins: Seq[Plugin] = Seq(
    new AvroPlugin,
    new XMLPlugin(pathQualifier),
    new ExcelPlugin(pathQualifier),
    new JDBCPlugin,
    new CassandraPlugin,
    new MongoPlugin,
    new ElasticSearchPlugin,
    new CobrixPlugin,
    new KafkaPlugin,
    new SaveIntoDataSourceCommandPlugin(this, pathQualifier),
    new SQLPlugin(pathQualifier, session),
    new LogicalRelationPlugin(this)
  )

}
