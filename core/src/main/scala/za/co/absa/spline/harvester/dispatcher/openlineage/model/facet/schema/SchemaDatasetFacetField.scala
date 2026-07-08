package za.co.absa.spline.harvester.dispatcher.openlineage.model.facet.schema

case class SchemaDatasetFacetField(
  name: String,
  `type`: Option[String],
  description: Option[String],
  fields: Seq[SchemaDatasetFacetField]
)
