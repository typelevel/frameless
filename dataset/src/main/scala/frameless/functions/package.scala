package frameless

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
    */
  def lit[A: TypedEncoder, T](value: A): TypedColumn[T, A] = {
    val encoder = TypedEncoder[A]

    if (ScalaReflection.isNativeType(encoder.jvmRepr) && encoder.catalystRepr == encoder.jvmRepr) {
      val expr = Literal(value, encoder.catalystRepr)
      new TypedColumn(expr)
    } else {
      val expr = FramelessLit(value, encoder)
      new TypedColumn(expr)
    }
  }
}
