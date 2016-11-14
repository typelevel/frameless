package frameless

import shapeless.Witness
import org.apache.spark.sql.Dataset

class ColumnSyntax[T](dataset: Dataset[T]) {
  def /[A: TypedEncoder](column: Witness.Lt[Symbol])
    (implicit exists: TypedColumn.Exists[T, column.T, A]): TypedColumn[A] = {
      val colExpr = dataset.col(column.value.name).as[A](TypedExpressionEncoder[A])
      new TypedColumn[A](colExpr)
    }
}
