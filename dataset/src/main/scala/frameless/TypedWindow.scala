package frameless

import frameless.ops.ColumnTypes
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{ Expression, SortOrder, UnspecifiedFrame, WindowFrame }
import org.apache.spark.sql.expressions.{ Window, WindowSpec }
import shapeless.ops.hlist.{ IsHCons, Mapper, ToTraversable }
import shapeless.{ HList, ProductArgs }

trait OrderedWindow
trait PartitionedWindow

class TypedWindow[T, A] private (
  partitionSpec: Seq[UntypedExpression[T]],
  orderSpec: Seq[UntypedExpression[T]],
  frame: WindowFrame //TODO. Really a rows or range between
) {

  def untyped: WindowSpec = Window
    .partitionBy(partitionSpec.map(e => new Column(e.expr)):_*)
    .orderBy(orderSpec.map(e => new Column(e.expr)):_*)
  //TODO: frame


  /* TODO: Do we want single column versions like we do for agg for better type inference?
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
    */

  object partitionBy extends ProductArgs {
    def applyProduct[U <: HList](columns: U)
      (implicit
        i1: ToTraversable.Aux[U, List, TypedColumn[T, _]]
      ): TypedWindow[T, A with PartitionedWindow] = {
      new TypedWindow[T, A with PartitionedWindow](
        partitionSpec = columns.toList[TypedColumn[T, _]],
        orderSpec = orderSpec,
        frame = frame
      )
    }
  }

  object orderBy extends ProductArgs {
    def applyProduct[U <: HList, O <: HList](columns: U)
      (implicit
        i0: Mapper.Aux[TypedSortedColumn.defaultAscendingPoly.type, U, O],
        i1: ToTraversable.Aux[O, List, TypedSortedColumn[T, _]]
      ): TypedWindow[T, A with OrderedWindow] = {
      new TypedWindow[T, A with OrderedWindow](
        partitionSpec = partitionSpec,
        orderSpec = i0(columns).toList[TypedSortedColumn[T, _]],
        frame = frame
      )
    }
  }
}

object TypedWindow {

  //TODO: Multiple columns. Hard because have to infer what `T` is
  def partitionBy[T](column: TypedColumn[T, _]): TypedWindow[T, PartitionedWindow] = {
    new TypedWindow[T, PartitionedWindow](
      partitionSpec = Seq(column),
      orderSpec = Seq.empty,
      frame = UnspecifiedFrame
    )
  }

  def orderBy[T](column: TypedSortedColumn[T, _]): TypedWindow[T, OrderedWindow] = {
    new TypedWindow[T, OrderedWindow](
      partitionSpec = Seq.empty,
      orderSpec = Seq(column),
      frame = UnspecifiedFrame
    )
  }
}

