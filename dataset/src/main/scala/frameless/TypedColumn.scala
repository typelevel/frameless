package frameless

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column, FramelessInternals}
import shapeless.ops.record.Selector
import shapeless._

import scala.annotation.implicitNotFound

sealed trait UntypedExpression[T] {
  def expr: Expression
}

/** Documentation marked "apache/spark" is thanks to apache/spark Contributors
  * at https://github.com/apache/spark, licensed under Apache v2.0 available at
  * http://www.apache.org/licenses/LICENSE-2.0
  */
sealed class TypedColumn[T, U](
  val expr: Expression
)(
  implicit
  val uencoder: TypedEncoder[U]
) extends UntypedExpression[T] { self =>

  def this(column: Column)(implicit uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }

  def untyped: Column = new Column(expr)

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === 1 )
    * }}}
    *
    * apache/spark
    */
  def ===(other: U): TypedColumn[T, Boolean] = {
    new TypedColumn[T, Boolean](untyped === other)
  }

  /** Equality test.
    * {{{
    *   df.filter( df.col('a) === df.col('b) )
    * }}}
    *
    * apache/spark
    */
  def ===(other: TypedColumn[T, U]): TypedColumn[T, Boolean] = {
    new TypedColumn[T, Boolean](untyped === other.untyped)
  }

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) plus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def plus(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] =
    new TypedColumn[T, U](self.untyped.plus(u.untyped))

  /** Sum of this expression and another expression.
    * {{{
    *   // The following selects the sum of a person's height and weight.
    *   people.select( people.col('height) + people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def +(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = plus(u)

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) minus people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def minus(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] =
    new TypedColumn[T, U](self.untyped.minus(u.untyped))

  /** Subtraction. Subtract the other expression from this expression.
    * {{{
    *   // The following selects the difference between people's height and their weight.
    *   people.select( people.col('height) - people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def -(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = minus(u)

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) multiply people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def multiply(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] =
    new TypedColumn[T, U](self.untyped.multiply(u.untyped))

  /** Multiplication of this expression and another expression.
    * {{{
    *   // The following multiplies a person's height by their weight.
    *   people.select( people.col('height) * people.col('weight) )
    * }}}
    *
    * apache/spark
    */
  def *(u: TypedColumn[T, U])(implicit n: CatalystNumeric[U]): TypedColumn[T, U] = multiply(u)

  /** Casts the column to a different type.
    * {{{
    *   df.select(df('a).cast[Int])
    * }}}
    */
  def cast[A: TypedEncoder](implicit c: CatalystCast[U, A]): TypedColumn[T, A] =
    new TypedColumn(self.untyped.cast(TypedEncoder[A].targetDataType))
}

sealed trait TypedAggregate[T, A] extends UntypedExpression[T] {
  def expr: Expression
  def aencoder: TypedEncoder[A]
}

sealed class TypedAggregateAndColumn[T, A, U](expr: Expression)(
  implicit
  val aencoder: TypedEncoder[A],
  uencoder: TypedEncoder[U]
) extends TypedColumn[T, U](expr) with TypedAggregate[T, A] {

  def this(column: Column)(implicit aencoder: TypedEncoder[A], uencoder: TypedEncoder[U]) {
    this(FramelessInternals.expr(column))
  }
}

object TypedColumn {
  /**
    * Evidence that type `T` has column `K` with type `V`.
    */
  @implicitNotFound(msg = "No column ${K} of type ${V} in ${T}")
  trait Exists[T, K, V]

  @implicitNotFound(msg = "No columns ${K} of type ${V} in ${T}")
  trait ExistsMany[T, K <: HList, V]

  object ExistsMany {
    implicit def deriveCons[T, KH, KT <: HList, V0, V1](
      implicit
      head: Exists[T, KH, V0],
      tail: ExistsMany[V0, KT, V1]
    ): ExistsMany[T, KH :: KT, V1] = new ExistsMany[T, KH :: KT, V1] {}

    implicit def deriveHNil[T, K, V](
      implicit
      head: Exists[T, K, V]
    ): ExistsMany[T, K :: HNil, V] = new ExistsMany[T, K :: HNil, V] {}
  }

  object Exists {
    def apply[T, V](column: Witness)(
      implicit
      exists: Exists[T, column.T, V]
    ): Exists[T, column.T, V] = exists

    implicit def deriveRecord[T, H <: HList, K, V](
      implicit
      lgen: LabelledGeneric.Aux[T, H],
      selector: Selector.Aux[H, K, V]
    ): Exists[T, K, V] = new Exists[T, K, V] {}
  }
}
