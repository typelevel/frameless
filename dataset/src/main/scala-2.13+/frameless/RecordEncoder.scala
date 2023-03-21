package frameless

import org.apache.spark.sql.FramelessInternals

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{ Invoke, NewInstance }
import org.apache.spark.sql.types._

import shapeless._
import shapeless.ops.hlist.IsHCons

import scala.reflect.ClassTag

class RecordEncoder[F, G <: HList, H <: HList]
  (implicit
    i0: LabelledGeneric.Aux[F, G],
    i1: DropUnitValues.Aux[G, H],
    i2: IsHCons[H],
    fields: RecordEncoderFields[H],
    newInstanceExprs: NewInstanceExprs[G],
    classTag: ClassTag[F]
  ) extends TypedEncoder[F] {
    def nullable: Boolean = false

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[F]

    def catalystRepr: DataType = {
      val structFields = fields.value.map { field =>
        StructField(
          name = field.name,
          dataType = field.encoder.catalystRepr,
          nullable = field.encoder.nullable,
          metadata = Metadata.empty
        )
      }

      StructType(structFields)
    }

    def toCatalyst(path: Expression): Expression = {
      val nameExprs = fields.value.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.value.map { field =>
        val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
        field.encoder.toCatalyst(fieldPath)
      }

      // the way exprs are encoded in CreateNamedStruct
      val exprs = nameExprs.zip(valueExprs).flatMap {
        case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
      }

      val createExpr = CreateNamedStruct(exprs)
      val nullExpr = Literal.create(null, createExpr.dataType)

      If(IsNull(path), nullExpr, createExpr)
    }

    def fromCatalyst(path: Expression): Expression = {
      val exprs = fields.value.map { field =>
        field.encoder.fromCatalyst(
          GetStructField(path, field.ordinal, Some(field.name)))
      }

      val newArgs = newInstanceExprs.from(exprs)
      val newExpr = NewInstance(
        classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)

      val nullExpr = Literal.create(null, jvmRepr)

      If(IsNull(path), nullExpr, newExpr)
    }
}
