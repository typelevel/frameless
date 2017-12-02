package frameless

import org.apache.spark.sql.catalyst.expressions.{ Expression, SortOrder, UnspecifiedFrame, WindowFrame }
import org.apache.spark.sql.expressions.{ Window, WindowSpec }

trait OrderedWindow
trait PartitionedWindow

class TypedWindow[T, A] private (
  partitionSpec: Seq[TypedColumn[T, _]],
  orderSpec: Seq[TypedSortedColumn[T, _]],
  frame: WindowFrame //TODO. Really a rows or range between
) {

  def untyped: WindowSpec = Window
    .partitionBy(partitionSpec.map(_.untyped):_*)
    .orderBy(orderSpec.map(_.untyped):_*)
  //TODO: frame


  //It's okay to call multiple times and overwrite the partition. I think
  def partitionBy[U](column: TypedColumn[T, U]): TypedWindow[T, A with PartitionedWindow] =
    new TypedWindow[T, A with PartitionedWindow](
      partitionSpec = Seq(column),
      orderSpec = orderSpec,
      frame = frame
    )

  def orderBy[U](column: TypedSortedColumn[T, U]): TypedWindow[T, A with OrderedWindow] =
    new TypedWindow[T, A with OrderedWindow](
      partitionSpec = partitionSpec,
      orderSpec = Seq(column),
      frame = frame
    )
}

object TypedWindow {

  //TODO: Support multiple columns
  def partitionBy[T, U](column: TypedColumn[T, U]): TypedWindow[T, PartitionedWindow] = {
    new TypedWindow[T, PartitionedWindow](
      partitionSpec = Seq(column),
      orderSpec = Seq.empty,
      frame = UnspecifiedFrame
    )
  }

  def orderBy[T, U](column: TypedSortedColumn[T, U]): TypedWindow[T, OrderedWindow] = {
    new TypedWindow[T, OrderedWindow](
      partitionSpec = Seq.empty,
      orderSpec = Seq(column),
      frame = UnspecifiedFrame
    )

  }
}

