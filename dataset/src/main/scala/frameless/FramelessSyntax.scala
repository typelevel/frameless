package frameless

import frameless.ops.ChainedJoinOps
import org.apache.spark.sql.{Column, DataFrame, Dataset}

trait FramelessSyntax {
  implicit class ColumnSyntax(self: Column) {
    def typedColumn[T, U: TypedEncoder]: TypedColumn[T, U] = new TypedColumn[T, U](self)
    def typedAggregate[T, U: TypedEncoder]: TypedAggregate[T, U] = new TypedAggregate[T, U](self)
  }

  implicit class DatasetSyntax[T: TypedEncoder](self: Dataset[T]) {
    def typed: TypedDataset[T] = TypedDataset.create[T](self)
  }

  implicit class DataframeSyntax(self: DataFrame){
    def unsafeTyped[T: TypedEncoder]: TypedDataset[T] = TypedDataset.createUnsafe(self)
  }

  implicit class ChainedJoinSyntax[T](ds: TypedDataset[T]) {
    def join[U](other: TypedDataset[U]): ChainedJoinOps[T, U] = new ChainedJoinOps[T, U](ds, other)
  }
}
