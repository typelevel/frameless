package frameless

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{ col => sparkCol }
import shapeless.Witness

package object functions extends Udf with UnaryFunctions {
  object aggregate extends AggregateFunctions

  def lit[A: TypedEncoder, T](value: A): TypedColumn[T, A] = {
    val encoder = TypedEncoder[A]

    if (ScalaReflection.isNativeType(encoder.sourceDataType) && encoder.targetDataType == encoder.sourceDataType) {
      val expr = Literal(value, encoder.targetDataType)
      new TypedColumn(expr)
    } else {
      val expr = FramelessLit(value, encoder)
      new TypedColumn(expr)
    }
  }

  def col[T, A](column: Witness.Lt[Symbol])(
    implicit
    exists: TypedColumn.Exists[T, column.T, A],
    encoder: TypedEncoder[A]): TypedColumn[T, A] = {
    val untypedExpr = sparkCol(column.value.name).as[A](TypedExpressionEncoder[A])
    new TypedColumn[T, A](untypedExpr)
  }
}
