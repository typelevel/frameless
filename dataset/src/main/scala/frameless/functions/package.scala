package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal

package object functions extends Udf with UnaryFunctions {
  object aggregate extends AggregateFunctions

  def lit[A: TypedEncoder, T](value: A): TypedColumn[A] = {
    val encoder = TypedEncoder[A]

    if (ScalaReflection.isNativeType(encoder.sourceDataType) && encoder.targetDataType == encoder.sourceDataType) {
      val expr = Literal(value, encoder.targetDataType)
      new TypedColumn(expr)
    } else {
      val expr = FramelessLit(value, encoder)
      new TypedColumn(expr)
    }
  }
}
