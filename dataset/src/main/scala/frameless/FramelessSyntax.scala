package frameless

import org.apache.spark.sql.{Column, DataFrame, Dataset}

trait FramelessSyntax {
  implicit class ColumnSyntax(self: Column) {
    def typed[T, U: TypedEncoder]: TypedColumn[T, U] = new TypedColumn[T, U](self)
  }

  implicit class DatasetSyntax[T: TypedEncoder](self: Dataset[T]) {
    def typed: TypedDataset[T] = TypedDataset.create[T](self)
  }

  implicit class DataframeSyntax(self: DataFrame){
    def unsafeTyped[T: TypedEncoder]: TypedDataset[T] = TypedDataset.createUnsafe(self)
  }
}
