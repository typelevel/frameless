package frameless

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{ UnspecifiedFrame, WindowFrame }
import org.apache.spark.sql.expressions.{ Window, WindowSpec }
import shapeless.ops.hlist.{ Mapper, ToTraversable }
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


  def partitionBy[U](
    column: TypedColumn[T, U]
  ): TypedWindow[T, A with PartitionedWindow] =
    partitionByMany(column)

  def partitionBy[U, V](
    column1: TypedColumn[T, U],
    column2: TypedColumn[T, V]
  ): TypedWindow[T, A with PartitionedWindow] =
    partitionByMany(column1, column2)

  def partitionBy[U, V, W](
    column1: TypedColumn[T, U],
    column2: TypedColumn[T, V],
    column3: TypedColumn[T,W]
  ): TypedWindow[T, A with PartitionedWindow] =
    partitionByMany(column1, column2, column3)

  object partitionByMany extends ProductArgs {
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

  def orderBy[U](
    column: SortedTypedColumn[T, U]
  ): TypedWindow[T, A with OrderedWindow] =
    orderByMany(column)

  def orderBy[U, V](
    column1: SortedTypedColumn[T, U],
    column2: SortedTypedColumn[T, V]
  ): TypedWindow[T, A with OrderedWindow] =
    orderByMany(column1, column2)

  def orderBy[U, V, W](
    column1: SortedTypedColumn[T, U],
    column2: SortedTypedColumn[T, V],
    column3: SortedTypedColumn[T, W]
  ): TypedWindow[T, A with OrderedWindow] =
    orderByMany(column1, column2, column3)

  object orderByMany extends ProductArgs {
    def applyProduct[U <: HList, O <: HList](columns: U)
      (implicit
        i0: Mapper.Aux[SortedTypedColumn.defaultAscendingPoly.type, U, O],
        i1: ToTraversable.Aux[O, List, SortedTypedColumn[T, _]]
      ): TypedWindow[T, A with OrderedWindow] = {
      new TypedWindow[T, A with OrderedWindow](
        partitionSpec = partitionSpec,
        orderSpec = i0(columns).toList[SortedTypedColumn[T, _]],
        frame = frame
      )
    }
  }
}

object TypedWindow {

  def orderBy[T](
    column: SortedTypedColumn[T, _]
  ): TypedWindow[T, OrderedWindow] =
    new orderByManyNew[T].apply(column) //TODO: This is some ugly syntax

  def orderBy[T](
    column1: SortedTypedColumn[T, _],
    column2: SortedTypedColumn[T, _]
  ): TypedWindow[T, OrderedWindow] =
    new orderByManyNew[T].apply(column1, column2)

  def orderBy[T](
    column1: SortedTypedColumn[T, _],
    column2: SortedTypedColumn[T, _],
    column3: SortedTypedColumn[T, _]
  ): TypedWindow[T, OrderedWindow] =
    new orderByManyNew[T].apply(column1, column2, column3)

  //Need different name because companion class has `orderByMany` defined as well
  //Need a class and not object in order to define what `T` is explicitly. Otherwise it's a mess
  //This makes for some pretty horrid syntax though.
  class orderByManyNew[T] extends ProductArgs {
    def applyProduct[U <: HList, O <: HList](columns: U)
      (implicit
        i0: Mapper.Aux[SortedTypedColumn.defaultAscendingPoly.type, U, O],
        i1: ToTraversable.Aux[O, List, SortedTypedColumn[T, _]]
      ): TypedWindow[T, OrderedWindow] = {
      new TypedWindow[T, OrderedWindow](
        partitionSpec = Seq.empty,
        orderSpec = i0(columns).toList[SortedTypedColumn[T, _]],
        frame = UnspecifiedFrame
      )
    }
  }

  def partitionBy[T](
    column: TypedColumn[T, _]
  ): TypedWindow[T, PartitionedWindow] =
    new partitionByManyNew[T].apply(column)

  def partitionBy[T](
    column1: TypedColumn[T, _],
    column2: TypedColumn[T, _]
  ): TypedWindow[T, PartitionedWindow] =
    new partitionByManyNew[T].apply(column1, column2)

  def partitionBy[T](
    column1: TypedColumn[T, _],
    column2: TypedColumn[T, _],
    column3: TypedColumn[T, _]
  ): TypedWindow[T, PartitionedWindow] =
    new partitionByManyNew[T].apply(column1, column2, column3)

  class partitionByManyNew[T] extends ProductArgs {
    def applyProduct[U <: HList](columns: U)
      (implicit
        i1: ToTraversable.Aux[U, List, TypedColumn[T, _]]
      ): TypedWindow[T, PartitionedWindow] = {
      new TypedWindow[T, PartitionedWindow](
        partitionSpec = columns.toList[TypedColumn[T, _]],
        orderSpec = Seq.empty,
        frame = UnspecifiedFrame
      )
    }
  }
}

