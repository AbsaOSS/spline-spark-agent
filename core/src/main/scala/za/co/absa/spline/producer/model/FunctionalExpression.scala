/**
 * Api Documentation
 * Api Documentation
 *
 * OpenAPI spec version: 1.0
 *
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */
package za.co.absa.spline.producer.model

case class FunctionalExpression (
  id: String,
  dataType: Option[Any],
  childRefs: Option[Seq[AttrOrExprRef]],
  extra: Option[Map[String, Any]],
  name: String,
  params: Option[Map[String, Any]]
)
