package frameless
package functions

import org.apache.spark.sql.{Column, functions => sparkFunctions}

import scala.math.Ordering

trait UnaryFunctions {
  /** Returns length of array or map.
    *
    * apache/spark
    */
  def size[T, A, V[_] : CatalystSizableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, Int] =
    new TypedColumn[T, Int](implicitly[CatalystSizableCollection[V]].sizeOp(column.untyped))

  /** Sorts the input array for the given column in ascending order, according to
    * the natural ordering of the array elements.
    *
    * apache/spark
    */
  def sort[T, A: Ordering, V[_] : CatalystSortableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, V[A]] =
    new TypedColumn[T, V[A]](implicitly[CatalystSortableCollection[V]].sortOp(column.untyped))(column.uencoder)
}

trait CatalystSizableCollection[V[_]] {
  def sizeOp(col: Column): Column
}

object CatalystSizableCollection {
  implicit def sizableVector: CatalystSizableCollection[Vector] = new CatalystSizableCollection[Vector] {
    def sizeOp(col: Column): Column = sparkFunctions.size(col)
  }
}

trait CatalystSortableCollection[V[_]] {
  def sortOp(col: Column): Column
}

object CatalystSortableCollection {
  implicit def sortableVector: CatalystSortableCollection[Vector] = new CatalystSortableCollection[Vector] {
    def sortOp(col: Column): Column = sparkFunctions.sort_array(col)
  }
}