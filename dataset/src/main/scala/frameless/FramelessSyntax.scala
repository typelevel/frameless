package frameless

import org.apache.spark.sql.{Column, Dataset}

trait FramelessSyntax {
  implicit class ColumnSyntax(self: Column) {
    def typed[T, U: TypedEncoder]: TypedColumn[T, U] = new TypedColumn[T, U](self)
  }

  implicit class DatasetSyntax[T: TypedEncoder](self: Dataset[T]) {
    def typed: TypedDataset[T] = TypedDataset.create[T](self)
  }
}
