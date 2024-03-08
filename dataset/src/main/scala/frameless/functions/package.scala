package frameless

import frameless.{ reflection => ScalaReflection }
import scala.reflect.ClassTag

import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.IsHCons
import shapeless.ops.record.{ Keys, Values }
import org.apache.spark.sql.catalyst.expressions.Literal

package object functions extends Udf with UnaryFunctions {

  object aggregate extends AggregateFunctions
  object nonAggregate extends NonAggregateFunctions

  /**
   * Creates a [[frameless.TypedAggregate]] of literal value. If A is to be encoded using an Injection make
   * sure the injection instance is in scope.
   *
   * apache/spark
   */
  def litAggr[A, T](
      value: A
    )(implicit
      i0: TypedEncoder[A],
      i1: Refute[IsValueClass[A]]
    ): TypedAggregate[T, A] =
    new TypedAggregate[T, A](lit(value).expr)

  /**
   * Creates a [[frameless.TypedColumn]] of literal value. If A is to be encoded using an Injection make
   * sure the injection instance is in scope.
   *
   * apache/spark
   *
   * @tparam A the literal value type
   * @tparam T the row type
   */
  def lit[A, T](
      value: A
    )(implicit
      encoder: TypedEncoder[A]
    ): TypedColumn[T, A] = {

    if (
      ScalaReflection.isNativeType(
        encoder.jvmRepr
      ) && encoder.catalystRepr == encoder.jvmRepr
    ) {
      val expr = Literal(value, encoder.catalystRepr)

      new TypedColumn(expr)
    } else {
      val expr = new Literal(value, encoder.jvmRepr)

      new TypedColumn[T, A](
        Lit(
          dataType = encoder.catalystRepr,
          nullable = encoder.nullable,
          show = () => value.toString,
          catalystExpr = encoder.toCatalyst(expr)
        )
      )
    }
  }

  /**
   * Creates a [[frameless.TypedColumn]] of literal value
   * for a Value class `A`.
   *
   * @tparam A the value class
   * @tparam T the row type
   */
  def litValue[
      A: IsValueClass,
      T,
      G <: ::[_, HNil],
      H <: ::[_ <: FieldType[_ <: Symbol, _], HNil],
      K <: Symbol,
      V,
      KS <: ::[_ <: Symbol, HNil],
      VS <: HList
    ](value: A
    )(implicit
      i0: LabelledGeneric.Aux[A, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons.Aux[H, _ <: FieldType[K, V], HNil],
      i3: Keys.Aux[H, KS],
      i4: Values.Aux[H, VS],
      i5: IsHCons.Aux[KS, K, HNil],
      i6: IsHCons.Aux[VS, V, HNil],
      i7: TypedEncoder[V],
      i8: ClassTag[A]
    ): TypedColumn[T, A] = {
    val expr = {
      val field: H = i1(i0.to(value))
      val v: V = i6.head(i4(field))

      new Literal(v, i7.jvmRepr)
    }

    implicit val enc: TypedEncoder[A] =
      RecordFieldEncoder.valueClass[A, G, H, K, V, KS].encoder

    new TypedColumn[T, A](
      Lit(
        dataType = i7.catalystRepr,
        nullable = i7.nullable,
        show = () => value.toString,
        i7.toCatalyst(expr)
      )
    )
  }

  /**
   * Creates a [[frameless.TypedColumn]] of literal value
   * for an optional Value class `A`.
   *
   * @tparam A the value class
   * @tparam T the row type
   */
  def litValue[
      A: IsValueClass,
      T,
      G <: ::[_, HNil],
      H <: ::[_ <: FieldType[_ <: Symbol, _], HNil],
      K <: Symbol,
      V,
      KS <: ::[_ <: Symbol, HNil],
      VS <: HList
    ](value: Option[A]
    )(implicit
      i0: LabelledGeneric.Aux[A, G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons.Aux[H, _ <: FieldType[K, V], HNil],
      i3: Keys.Aux[H, KS],
      i4: Values.Aux[H, VS],
      i5: IsHCons.Aux[KS, K, HNil],
      i6: IsHCons.Aux[VS, V, HNil],
      i7: TypedEncoder[V],
      i8: ClassTag[A]
    ): TypedColumn[T, Option[A]] = {
    val expr = value match {
      case Some(some) => {
        val field: H = i1(i0.to(some))
        val v: V = i6.head(i4(field))

        new Literal(v, i7.jvmRepr)
      }

      case _ =>
        Literal.create(null, i7.jvmRepr)
    }

    implicit val enc: TypedEncoder[A] =
      RecordFieldEncoder.valueClass[A, G, H, K, V, KS].encoder

    new TypedColumn[T, Option[A]](
      Lit(
        dataType = i7.catalystRepr,
        nullable = true,
        show = () => value.toString,
        i7.toCatalyst(expr)
      )
    )
  }
}
