package frameless

import scala.reflect.ClassTag

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{
  Invoke,
  NewInstance,
  UnwrapOption,
  WrapOption
}
import org.apache.spark.sql.types._

import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.IsHCons
import shapeless.ops.record.Keys

case class RecordEncoderField(
    ordinal: Int,
    name: String,
    encoder: TypedEncoder[_]
)

trait RecordEncoderFields[T <: HList] extends Serializable {
  def value: List[RecordEncoderField]
}

object RecordEncoderFields {

  implicit def deriveRecordLast[K <: Symbol, H](
      implicit key: Witness.Aux[K],
      head: RecordFieldEncoder[H]): RecordEncoderFields[FieldType[K, H] :: HNil] =
    new RecordEncoderFields[FieldType[K, H] :: HNil] {
      def value: List[RecordEncoderField] = fieldEncoder[K, H] :: Nil
    }

  implicit def deriveRecordCons[K <: Symbol, H, T <: HList](
      implicit key: Witness.Aux[K],
      head: RecordFieldEncoder[H],
      tail: RecordEncoderFields[T]): RecordEncoderFields[FieldType[K, H] :: T] =
    new RecordEncoderFields[FieldType[K, H] :: T] {
      def value: List[RecordEncoderField] =
        fieldEncoder[K, H] :: tail.value.map(x => x.copy(ordinal = x.ordinal + 1))
    }

  private def fieldEncoder[K <: Symbol, H](
      implicit key: Witness.Aux[K],
      e: RecordFieldEncoder[H]): RecordEncoderField =
    RecordEncoderField(0, key.value.name, e.encoder)
}

/**
 * Assists the generation of constructor call parameters from a labelled generic representation.
 * As Unit typed fields were removed earlier, we need to put back unit literals in the
 * appropriate positions.
 *
 * @tparam T
 *   labelled generic representation of type fields
 */
trait NewInstanceExprs[T <: HList] extends Serializable {
  def from(exprs: List[Expression]): Seq[Expression]
}

object NewInstanceExprs {

  implicit def deriveHNil: NewInstanceExprs[HNil] = new NewInstanceExprs[HNil] {
    def from(exprs: List[Expression]): Seq[Expression] = Nil
  }

  implicit def deriveUnit[K <: Symbol, T <: HList](
      implicit tail: NewInstanceExprs[T]): NewInstanceExprs[FieldType[K, Unit] :: T] =
    new NewInstanceExprs[FieldType[K, Unit] :: T] {
      def from(exprs: List[Expression]): Seq[Expression] =
        Literal.fromObject(()) +: tail.from(exprs)
    }

  implicit def deriveNonUnit[K <: Symbol, V, T <: HList](
      implicit notUnit: V =:!= Unit,
      tail: NewInstanceExprs[T]): NewInstanceExprs[FieldType[K, V] :: T] =
    new NewInstanceExprs[FieldType[K, V] :: T] {
      def from(exprs: List[Expression]): Seq[Expression] = exprs.head +: tail.from(exprs.tail)
    }
}

/**
 * Drops fields with Unit type from labelled generic representation of types.
 *
 * @tparam L
 *   labelled generic representation of type fields
 */
trait DropUnitValues[L <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object DropUnitValues {
  def apply[L <: HList](
      implicit dropUnitValues: DropUnitValues[L]): Aux[L, dropUnitValues.Out] = dropUnitValues

  type Aux[L <: HList, Out0 <: HList] = DropUnitValues[L] { type Out = Out0 }

  implicit def deriveHNil[H]: Aux[HNil, HNil] = new DropUnitValues[HNil] {
    type Out = HNil
    def apply(l: HNil): Out = HNil
  }

  implicit def deriveUnit[K <: Symbol, T <: HList, OutT <: HList](
      implicit
      dropUnitValues: DropUnitValues.Aux[T, OutT]): Aux[FieldType[K, Unit] :: T, OutT] =
    new DropUnitValues[FieldType[K, Unit] :: T] {
      type Out = OutT
      def apply(l: FieldType[K, Unit] :: T): Out = dropUnitValues(l.tail)
    }

  implicit def deriveNonUnit[K <: Symbol, V, T <: HList, OutH, OutT <: HList](
      implicit nonUnit: V =:!= Unit,
      dropUnitValues: DropUnitValues.Aux[T, OutT])
      : Aux[FieldType[K, V] :: T, FieldType[K, V] :: OutT] =
    new DropUnitValues[FieldType[K, V] :: T] {
      type Out = FieldType[K, V] :: OutT
      def apply(l: FieldType[K, V] :: T): Out = l.head :: dropUnitValues(l.tail)
    }
}

class RecordEncoder[F, G <: HList, H <: HList](
    implicit i0: LabelledGeneric.Aux[F, G],
    i1: DropUnitValues.Aux[G, H],
    i2: IsHCons[H],
    fields: Lazy[RecordEncoderFields[H]],
    newInstanceExprs: Lazy[NewInstanceExprs[G]],
    classTag: ClassTag[F])
    extends TypedEncoder[F] {
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
    val nameExprs = fields.value.value.map { field => Literal(field.name) }

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
      field.encoder.fromCatalyst(GetStructField(path, field.ordinal, Some(field.name)))
    }

    val newArgs = newInstanceExprs.value.from(exprs)
    val newExpr = NewInstance(classTag.runtimeClass, newArgs, jvmRepr, propagateNull = true)

    val nullExpr = Literal.create(null, jvmRepr)
    If(IsNull(path), nullExpr, newExpr)
  }
}

final class RecordFieldEncoder[T](val encoder: TypedEncoder[T]) extends Serializable

object RecordFieldEncoder extends RecordFieldEncoderLowPriority {

  /**
   * @tparam F the value class
   * @tparam G the single field of the value class
   * @tparam H the single field of the value class (with guarantee it's not a `Unit` value)
   * @tparam K the key type for the fields
   * @tparam V the inner value type
   */
  implicit def optionValueClass[
      F: IsValueClass,
      G <: ::[_, HNil],
      H <: ::[_ <: FieldType[_ <: Symbol, _], HNil],
      K <: Symbol,
      V,
      KS <: ::[_ <: Symbol, HNil]](
      implicit i0: LabelledGeneric.Aux[F, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons.Aux[H, _ <: FieldType[K, V], HNil],
      i3: Keys.Aux[H, KS],
      i4: IsHCons.Aux[KS, K, HNil],
      i5: TypedEncoder[V],
      i6: ClassTag[F]): RecordFieldEncoder[Option[F]] =
    RecordFieldEncoder[Option[F]](new TypedEncoder[Option[F]] {
      val nullable = true

      val jvmRepr = ObjectType(classOf[Option[F]])

      @inline def catalystRepr: DataType = i5.catalystRepr

      val innerJvmRepr = ObjectType(i6.runtimeClass)

      def fromCatalyst(path: Expression): Expression = {
        val javaValue = i5.fromCatalyst(path)
        val value = NewInstance(i6.runtimeClass, Seq(javaValue), innerJvmRepr)

        WrapOption(value, innerJvmRepr)
      }

      @inline def toCatalyst(path: Expression): Expression = {
        val value = UnwrapOption(innerJvmRepr, path)

        val fieldName = i4.head(i3()).name
        val javaValue = Invoke(value, fieldName, i5.jvmRepr, Nil)

        i5.toCatalyst(javaValue)
      }
    })

  /**
   * @tparam F the value class
   * @tparam G the single field of the value class
   * @tparam H the single field of the value class (with guarantee it's not a `Unit` value)
   * @tparam V the inner value type
   */
  implicit def valueClass[F: IsValueClass, G <: ::[_, HNil], H <: ::[_, HNil], V](
      implicit i0: LabelledGeneric.Aux[F, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons.Aux[H, _ <: FieldType[_, V], HNil],
      i3: TypedEncoder[V],
      i4: ClassTag[F]): RecordFieldEncoder[F] = RecordFieldEncoder[F](new TypedEncoder[F] {
    def nullable = i3.nullable

    def jvmRepr = i3.jvmRepr

    def catalystRepr: DataType = i3.catalystRepr

    def fromCatalyst(path: Expression): Expression =
      i3.fromCatalyst(path)

    @inline def toCatalyst(path: Expression): Expression =
      i3.toCatalyst(path)
  })
}

private[frameless] sealed trait RecordFieldEncoderLowPriority {
  implicit def apply[T](implicit e: TypedEncoder[T]): RecordFieldEncoder[T] =
    new RecordFieldEncoder[T](e)
}
