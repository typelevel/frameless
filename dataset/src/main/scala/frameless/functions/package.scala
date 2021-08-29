package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal

package object functions
    extends Udf with UnaryFunctions with LowPriorityFunctions {

  object aggregate extends AggregateFunctions
  object nonAggregate extends NonAggregateFunctions

  /** Creates a [[frameless.TypedAggregate]] of literal value. If A is to be encoded using an Injection make
    * sure the injection instance is in scope.
    *
    * apache/spark
    */
  def litAggr[A: TypedEncoder, T](value: A): TypedAggregate[T, A] =
    new TypedAggregate[T,A](lit(value).expr)
}

private[frameless] sealed trait LowPriorityFunctions {

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
}
