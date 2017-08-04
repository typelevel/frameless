package frameless
package functions

import org.apache.spark.sql.{Column, functions => sparkFunctions}

import scala.math.Ordering

trait UnaryFunctions {
  /** Returns length of array or map.
    *
    * apache/spark
    */
  def size[T, A, V[_] : CatalystSizableCollection](column: TypedColumn[V[A]]): TypedColumn[Int] =
    new TypedColumn[Int](implicitly[CatalystSizableCollection[V]].sizeOp(column.untyped))

  /** Sorts the input array for the given column in ascending order, according to
    * the natural ordering of the array elements.
    *
    * apache/spark
    */
  def sortAscending[T, A: Ordering, V[_] : CatalystSortableCollection](column: TypedColumn[V[A]]): TypedColumn[V[A]] =
    new TypedColumn[V[A]](implicitly[CatalystSortableCollection[V]].sortOp(column.untyped, sortAscending = true))(column.uencoder)

  /** Sorts the input array for the given column in descending order, according to
    * the natural ordering of the array elements.
    *
    * apache/spark
    */
  def sortDescending[T, A: Ordering, V[_] : CatalystSortableCollection](column: TypedColumn[V[A]]): TypedColumn[V[A]] =
    new TypedColumn[V[A]](implicitly[CatalystSortableCollection[V]].sortOp(column.untyped, sortAscending = false))(column.uencoder)


  /** Creates a new row for each element in the given collection. The column types
    * eligible for this operation are constrained by CatalystExplodableCollection.
    *
    * apache/spark
    */
  def explode[T, A: TypedEncoder, V[_] : CatalystExplodableCollection](column: TypedColumn[V[A]]): TypedColumn[A] =
    new TypedColumn[A](sparkFunctions.explode(column.untyped))
}

trait CatalystSizableCollection[V[_]] {
  def sizeOp(col: Column): Column
}

object CatalystSizableCollection {
  implicit def sizableVector: CatalystSizableCollection[Vector] = new CatalystSizableCollection[Vector] {
    def sizeOp(col: Column): Column = sparkFunctions.size(col)
  }
}

trait CatalystExplodableCollection[V[_]]

object CatalystExplodableCollection {
  implicit def explodableVector: CatalystExplodableCollection[Vector] = new CatalystExplodableCollection[Vector] {}
}

trait CatalystSortableCollection[V[_]] {
  def sortOp(col: Column, sortAscending: Boolean): Column
}

object CatalystSortableCollection {
  implicit def sortableVector: CatalystSortableCollection[Vector] = new CatalystSortableCollection[Vector] {
    def sortOp(col: Column, sortAscending: Boolean): Column = sparkFunctions.sort_array(col, sortAscending)
  }
}
