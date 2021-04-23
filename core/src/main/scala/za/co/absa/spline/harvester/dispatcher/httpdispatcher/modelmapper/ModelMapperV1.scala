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

package za.co.absa.spline.harvester.dispatcher.httpdispatcher.modelmapper

import za.co.absa.spline.harvester.converter.ExpressionConverter.{ExprExtra, ExprV1}
import za.co.absa.spline.producer.model.{v1_0, v1_1}


object ModelMapperV1 extends ModelMapper {

  object FieldsV1 {

    object ExecutionEventExtra {
      val DurationNs = "durationNs"
    }

    object ExecutionPlanExtra {
      val AppName = "appName"
      val Attributes = "attributes"
    }

    object OperationExtras {
      val Name = "name"
    }

    object Expression {
      val TypeHint: String = ExprV1.TypeHint
      val RefId = "refId"
      val Value = "value"
      val DataTypeId = "dataTypeId"
      val ExprType = "exprType"
      val Name = "name"
      val Symbol = "symbol"
      val Params = "params"
      val Child = "child"
      val Children = "children"
      val Alias = "alias"
    }

  }

  object ExprTypesV1 {
    val AttrRef = "expr.AttrRef"
    val Literal = "expr.Literal"
  }

  /**
   * Convert ExecutionPlan v1.1 to ExecutionPlan v1.0
   */
  override def toDTO(plan: v1_1.ExecutionPlan): AnyRef = {
    val exprById =
      plan.expressions
        .map(expressions =>
          (expressions.constants.getOrElse(Nil).groupBy(_.id) ++
            expressions.functions.getOrElse(Nil).groupBy(_.id)
            ).mapValues(_.head))
        .getOrElse(Map.empty)

    def toV1Operations(operations: v1_1.Operations) =
      v1_0.Operations(
        toV1WriteOperation(operations.write),
        operations.reads.map(ops => ops.map(toV1ReadOperation)),
        operations.other.map(ops => ops.map(toV1DataOperation))
      )

    def toV1WriteOperation(operation: v1_1.WriteOperation) =
      v1_0.WriteOperation(
        operation.outputSource,
        None,
        operation.append,
        operation.id.toInt,
        operation.childIds.map(_.toInt),
        operation.params.map(toV1OperationParams),
        Some(operation.extra.getOrElse(Map.empty) + (FieldsV1.OperationExtras.Name -> operation.name))
      )

    def toV1ReadOperation(operation: v1_1.ReadOperation) =
      v1_0.ReadOperation(
        Nil,
        operation.inputSources,
        operation.id.toInt,
        operation.output,
        operation.params.map(toV1OperationParams),
        Some(operation.extra.getOrElse(Map.empty) + (FieldsV1.OperationExtras.Name -> operation.name))
      )

    def toV1DataOperation(operation: v1_1.DataOperation) =
      v1_0.DataOperation(
        operation.id.toInt,
        operation.childIds.map(ids => ids.map(_.toInt)),
        operation.output,
        operation.params.map(toV1OperationParams),
        Some(operation.extra.getOrElse(Map.empty) + (FieldsV1.OperationExtras.Name -> operation.name))
      )

    def toV1SystemInfo(nav: v1_1.NameAndVersion) = v1_0.SystemInfo(nav.name, nav.version)

    def toV1AgentInfo(nav: v1_1.NameAndVersion) = v1_0.AgentInfo(nav.name, nav.version)

    def toV1OperationParams(params: Map[String, Any]): Map[String, Any] = {
      def convert(x: Any): Any = x match {
        case Some(v) => convert(v)
        case xs: Seq[_] => xs.map(convert)
        case ys: Map[String, _] => ys.mapValues(convert)
        case ref: v1_1.AttrOrExprRef => refToV1Expression(ref)
        case _ => x
      }

      params.mapValues(convert)
    }

    def refToV1Expression(ref: v1_1.AttrOrExprRef): Map[String, Any] = ref match {
      case v1_1.AttrOrExprRef(None, Some(exprId)) => exprToV1Expression(exprById(exprId))
      case v1_1.AttrOrExprRef(Some(attrId), None) => Map(
        FieldsV1.Expression.TypeHint -> ExprTypesV1.AttrRef,
        FieldsV1.Expression.RefId -> attrId
      )
    }

    def exprToV1Expression(expr: Product): Map[String, Any] = expr match {
      case lit: v1_1.Literal => Map(
        FieldsV1.Expression.TypeHint -> ExprTypesV1.Literal,
        FieldsV1.Expression.Value -> lit.value,
        FieldsV1.Expression.DataTypeId -> lit.dataType
      )
      case fun: v1_1.FunctionalExpression =>
        fun.extra.map(_ (FieldsV1.Expression.TypeHint)).foldLeft(Map.empty[String, Any]) {
          case (exprV1, typeHint) => (
            exprV1
              + (FieldsV1.Expression.TypeHint -> typeHint)
              ++ (
              typeHint match {
                case ExprV1.Types.Generic => Map(
                  FieldsV1.Expression.Name -> fun.name,
                  FieldsV1.Expression.DataTypeId -> fun.dataType,
                  FieldsV1.Expression.Children -> fun.childRefs.map(children => children.map(refToV1Expression)),
                  FieldsV1.Expression.ExprType -> fun.extra.map(_.get(ExprExtra.SimpleClassName)),
                  FieldsV1.Expression.Params -> fun.params
                )
                case ExprV1.Types.GenericLeaf => Map(
                  FieldsV1.Expression.Name -> fun.name,
                  FieldsV1.Expression.DataTypeId -> fun.dataType,
                  FieldsV1.Expression.ExprType -> fun.extra.map(_.get(ExprExtra.SimpleClassName)),
                  FieldsV1.Expression.Params -> fun.params
                )
                case ExprV1.Types.Alias => Map(
                  FieldsV1.Expression.Alias -> fun.name,
                  FieldsV1.Expression.Child -> fun.childRefs.map(children => refToV1Expression(children.head))
                )
                case ExprV1.Types.Binary => Map(
                  FieldsV1.Expression.Symbol -> fun.extra.map(_.get(ExprExtra.Symbol)),
                  FieldsV1.Expression.DataTypeId -> fun.dataType,
                  FieldsV1.Expression.Children -> fun.childRefs.map(children => children.map(refToV1Expression))
                )
                case ExprV1.Types.UDF => Map(
                  FieldsV1.Expression.Name -> fun.name,
                  FieldsV1.Expression.DataTypeId -> fun.dataType,
                  FieldsV1.Expression.Children -> fun.childRefs.map(children => children.map(refToV1Expression))
                )
                case ExprV1.Types.UntypedExpression => Map(
                  FieldsV1.Expression.Name -> fun.name,
                  FieldsV1.Expression.Children -> fun.childRefs.map(children => children.map(refToV1Expression)),
                  FieldsV1.Expression.ExprType -> fun.extra.map(_.get(ExprExtra.SimpleClassName)),
                  FieldsV1.Expression.Params -> fun.params
                )
              })
            )
        }
    }

    def toV1Attribute(attr: v1_1.Attribute): Map[String, Any] = Map(
      "id" -> attr.id,
      "name" -> attr.name,
      "dataTypeId" -> attr.dataType
    )

    v1_0.ExecutionPlan(
      plan.id,
      toV1Operations(plan.operations),
      toV1SystemInfo(plan.systemInfo),
      plan.agentInfo.map(toV1AgentInfo),
      Some(plan.extraInfo.getOrElse(Map.empty)
        + (FieldsV1.ExecutionPlanExtra.AppName -> plan.name)
        + (FieldsV1.ExecutionPlanExtra.Attributes -> plan.attributes.map(_.map(toV1Attribute)))
      )
    )
  }

  /**
   * Convert ExecutionEvent v1.1 to ExecutionEvent v1.0
   */
  override def toDTO(event: v1_1.ExecutionEvent): AnyRef =
    v1_0.ExecutionEvent(
      event.planId,
      event.timestamp,
      event.error,
      Some(event.extra.getOrElse(Map.empty) + (FieldsV1.ExecutionEventExtra.DurationNs -> event.durationNs))
    )
}
