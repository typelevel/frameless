package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal

package object functions extends Udf with UnaryFunctions {
  object aggregate extends AggregateFunctions
  object nonAggregate extends NonAggregateFunctions

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
