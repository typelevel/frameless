package frameless
package functions

import org.apache.spark.sql.{Column, functions => vanilla}

import scala.math.Ordering

trait UnaryFunctions {
  def size[T, A, V[_]: CatalystSizableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, Int] =
    new TypedColumn[T, Int](implicitly[CatalystSizableCollection[V]].sizeOp(column.untyped))

  def sort[T, A: Ordering, V[_]: CatalystSortableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, V[A]] =
    new TypedColumn[T, V[A]](implicitly[CatalystSortableCollection[V]].sortOp(column.untyped))(column.uencoder)
}

trait CatalystSizableCollection[V[_]] {
  def sizeOp(col: Column): Column
}

object CatalystSizableCollection {
  implicit def sizableVector: CatalystSizableCollection[Vector] = new CatalystSizableCollection[Vector] {
    def sizeOp(col: Column): Column = vanilla.size(col)
  }
}

trait CatalystSortableCollection[V[_]] {
  def sortOp(col: Column): Column
}

object CatalystSortableCollection {
  implicit def sortableVector: CatalystSortableCollection[Vector] = new CatalystSortableCollection[Vector] {
    def sortOp(col: Column): Column = vanilla.sort_array(col)
  }
}