package frameless

import scala.reflect.ClassTag

import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.IsHCons
import shapeless.ops.record.Values

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal

package object functions extends Udf with UnaryFunctions {

  object aggregate extends AggregateFunctions
  object nonAggregate extends NonAggregateFunctions

  /** Creates a [[frameless.TypedAggregate]] of literal value. If A is to be encoded using an Injection make
    * sure the injection instance is in scope.
    *
    * apache/spark
    */
  def litAggr[A: TypedEncoder, T](value: A): TypedAggregate[T, A] =
    new TypedAggregate[T,A](lit(value).expr)

  /** Creates a [[frameless.TypedColumn]] of literal value. If A is to be encoded using an Injection make
    * sure the injection instance is in scope.
    *
    * apache/spark
    *
    * @tparam A the literal value type
    * @tparam T the row type
    */
  def lit[A, T](value: A)(
    implicit encoder: TypedEncoder[A]): TypedColumn[T, A] = {

    if (ScalaReflection.isNativeType(encoder.jvmRepr) && encoder.catalystRepr == encoder.jvmRepr) {
      val expr = Literal(value, encoder.catalystRepr)
      new TypedColumn(expr)
    } else {
      val expr = new Literal(value, encoder.jvmRepr)

      new TypedColumn[T, A](
        new functions.Lit(
          dataType = encoder.catalystRepr,
          nullable = encoder.nullable,
          toCatalyst = encoder.toCatalyst(expr).genCode(_),
          show = value.toString
        )
      )
    }
  }

  /** Creates a [[frameless.TypedColumn]] of literal value 
    * for a Value class `A`.
    * 
    * @tparam A the value class
    * @tparam T the row type
    */
  def litValue[A <: AnyVal, T, G <: ::[_, HNil], H <: ::[_ <: FieldType[_ <: Symbol, _], HNil], V, VS <: HList](value: A)(
    implicit
      i0: LabelledGeneric.Aux[A, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons.Aux[H, _ <: FieldType[_, V], HNil],
      i3: Values.Aux[H, VS],
      i4: IsHCons.Aux[VS, V, HNil],
      i5: TypedEncoder[V],
      i6: ClassTag[A]
  ): TypedColumn[T, A] = {
    val expr = {
      val field: H = i1(i0.to(value))
      val v: V = i4.head(i3(field))

      new Literal(v, i5.jvmRepr)
    }

    implicit val enc: TypedEncoder[A] =
      RecordFieldEncoder.valueClass[A, G, H, V].encoder

    new TypedColumn[T, A](
      new Lit(
        dataType = i5.catalystRepr,
        nullable = i5.nullable,
        toCatalyst = i5.toCatalyst(expr).genCode(_),
        show = value.toString
      )
    )
  }
}
