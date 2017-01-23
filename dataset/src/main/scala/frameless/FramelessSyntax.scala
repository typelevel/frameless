package frameless

import org.apache.spark.sql.{Column, Dataset}

trait FramelessSyntax {
  implicit class ColumnSyntax(self: Column) {
    def typed[T, U: TypedEncoder]: TypedColumn[T, U] = new TypedColumn[T, U](self)
  }

  implicit class DatasetSyntax[T](self: Dataset[T]) {
    def typed(implicit ev: TypedEncoder[T]): TypedDataset[T] = TypedDataset.create[T](self)
    def toTyped[U: TypedEncoder]: TypedDataset[U] = TypedDataset.create(self.as(TypedExpressionEncoder[U]))
  }
}
