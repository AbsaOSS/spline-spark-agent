package za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.schema

import za.co.absa.spline.harvester.dispatcher.openlineage.model.openlineage.v2_0_2.DatasetFacet

case class SchemaDatasetFacet(
  _producer: String,
  _schemaURL: String,
  _deleted: Option[Boolean] = None,
  fields: Seq[SchemaDatasetFacetField]
) extends DatasetFacet(_producer, _schemaURL, _deleted)
