package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.types._
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.IsHCons

import scala.reflect.ClassTag

case class RecordEncoderField(
  ordinal: Int,
  name: String,
  encoder: TypedEncoder[_]
)

trait RecordEncoderFields[T <: HList] extends Serializable {
  def value: List[RecordEncoderField]
}

object RecordEncoderFields {

  implicit def deriveRecordLast[K <: Symbol, H]
    (implicit
      key: Witness.Aux[K],
      head: TypedEncoder[H]
    ): RecordEncoderFields[FieldType[K, H] :: HNil] = new RecordEncoderFields[FieldType[K, H] :: HNil] {
      def value: List[RecordEncoderField] = RecordEncoderField(0, key.value.name, head) :: Nil
    }

  implicit def deriveRecordCons[K <: Symbol, H, T <: HList]
    (implicit
      key: Witness.Aux[K],
      head: TypedEncoder[H],
      tail: RecordEncoderFields[T]
    ): RecordEncoderFields[FieldType[K, H] :: T] = new RecordEncoderFields[FieldType[K, H] :: T] {
      def value: List[RecordEncoderField] = {
        val fieldName = key.value.name
        val fieldEncoder = RecordEncoderField(0, fieldName, head)

        fieldEncoder :: tail.value.map(x => x.copy(ordinal = x.ordinal + 1))
      }
    }
}

/**
  * Assists the generation of constructor call parameters from a labelled generic representation.
  * As Unit typed fields were removed earlier, we need to put back unit literals in the  appropriate positions.
  *
  * @tparam T labelled generic representation of type fields
  */
trait NewInstanceExprs[T <: HList] extends Serializable {
  def from(exprs: List[Expression]): Seq[Expression]
}

object NewInstanceExprs {

  implicit def deriveHNil: NewInstanceExprs[HNil] = new NewInstanceExprs[HNil] {
    def from(exprs: List[Expression]): Seq[Expression] = Nil
  }

  implicit def deriveUnit[K <: Symbol, T <: HList]
    (implicit
      tail: NewInstanceExprs[T]
    ): NewInstanceExprs[FieldType[K, Unit] :: T] = new NewInstanceExprs[FieldType[K, Unit] :: T] {
      def from(exprs: List[Expression]): Seq[Expression] =
        Literal.fromObject(()) +: tail.from(exprs)
    }

  implicit def deriveNonUnit[K <: Symbol, V , T <: HList]
    (implicit
      notUnit: V =:!= Unit,
      tail: NewInstanceExprs[T]
    ): NewInstanceExprs[FieldType[K, V] :: T] = new NewInstanceExprs[FieldType[K, V] :: T] {
      def from(exprs: List[Expression]): Seq[Expression] = exprs.head +: tail.from(exprs.tail)
    }
}

/**
  * Drops fields with Unit type from labelled generic representation of types.
  *
  * @tparam L labelled generic representation of type fields
  */
trait DropUnitValues[L <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object DropUnitValues {
  def apply[L <: HList](implicit dropUnitValues: DropUnitValues[L]): Aux[L, dropUnitValues.Out] = dropUnitValues

  type Aux[L <: HList, Out0 <: HList] = DropUnitValues[L] { type Out = Out0 }

  implicit def deriveHNil[H]: Aux[HNil, HNil] = new DropUnitValues[HNil] {
    type Out = HNil
    def apply(l: HNil): Out = HNil
  }

  implicit def deriveUnit[K <: Symbol, T <: HList, OutT <: HList]
    (implicit
      dropUnitValues : DropUnitValues.Aux[T, OutT]
    ): Aux[FieldType[K, Unit] :: T, OutT] = new DropUnitValues[FieldType[K, Unit] :: T] {
      type Out = OutT
      def apply(l : FieldType[K, Unit] :: T): Out = dropUnitValues(l.tail)
    }

  implicit def deriveNonUnit[K <: Symbol, V, T <: HList, OutH, OutT <: HList]
    (implicit
      nonUnit: V =:!= Unit,
      dropUnitValues : DropUnitValues.Aux[T, OutT]
    ): Aux[FieldType[K, V] :: T, FieldType[K, V] :: OutT] = new DropUnitValues[FieldType[K, V] :: T] {
      type Out = FieldType[K, V] :: OutT
      def apply(l : FieldType[K, V] :: T): Out = l.head :: dropUnitValues(l.tail)
    }
}

class RecordEncoder[F, G <: HList, H <: HList]
  (implicit
    i0: LabelledGeneric.Aux[F, G],
    i1: DropUnitValues.Aux[G, H],
    i2: IsHCons[H],
    fields: Lazy[RecordEncoderFields[H]],
    newInstanceExprs: Lazy[NewInstanceExprs[G]],
    classTag: ClassTag[F]
  ) extends TypedEncoder[F] {
    def nullable: Boolean = false

    def jvmRepr: DataType = FramelessInternals.objectTypeFor[F]

    def catalystRepr: DataType = {
      val structFields = fields.value.value.map { field =>
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
      val nameExprs = fields.value.value.map { field =>
        Literal(field.name)
      }

      val valueExprs = fields.value.value.map { field =>
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
      val exprs = fields.value.value.map { field =>
        val fieldPath = (field, path) match {
          /*case (RecordEncoderField(_, _, r), BoundReference(ordinal, dataType, nullable) )
            if (r.getClass.getName == "frameless.RecordEncoder"
              || r.getClass.getName == "frameless.InjectEncoder"
              ) && fields.value.value.size == 1 =>
            GetStructField(path, field.ordinal, Some(field.name))*/
          case (_, BoundReference(ordinal, dataType, nullable) ) =>
            GetColumnByOrdinal(field.ordinal, field.encoder.jvmRepr)
          /*case (RecordEncoderField(_, _, r), other)
            if r.getClass.getName == "frameless.OptionEncoder" =>
            //UnresolvedAttribute("a.a.a")
            GetStructField(path, field.ordinal, Some(field.name))*/
          case (_, other) =>
            GetStructField(path, field.ordinal, Some(field.name))
        }
        field.encoder.fromCatalyst(fieldPath)
      }
      val nullExpr = Literal.create(null, jvmRepr)

      // UnresolvedExtractValue( UnresolvedAttribute(Seq(field.name)), Literal(field.name))
      val newArgs = newInstanceExprs.value.from(exprs)
      val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)
      If(IsNull(path), nullExpr, newExpr)

/*      path match {
        case BoundReference(0, _, _) =>
          newExpr
        case _ => {
          If(IsNull(path), nullExpr, newExpr)
        }
      } */
    }
}
