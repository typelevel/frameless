package frameless
package functions

import org.apache.spark.sql.{Column, functions => sparkFunctions}

import scala.math.Ordering

trait UnaryFunctions {
  /** Returns length of array
    *
    * apache/spark
    */
  def size[T, A, V[_] : CatalystSizableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, Int] =
    new TypedColumn[T, Int](implicitly[CatalystSizableCollection[V]].sizeOp(column.untyped))

  /** Returns length of Map
    *
    * apache/spark
    */
  def size[T, A, B](column: TypedColumn[T, Map[A, B]]): TypedColumn[T, Int] =
    new TypedColumn[T, Int](sparkFunctions.size(column.untyped))

  /** Sorts the input array for the given column in ascending order, according to
    * the natural ordering of the array elements.
    *
    * apache/spark
    */
  def sortAscending[T, A: Ordering, V[_] : CatalystSortableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, V[A]] =
    new TypedColumn[T, V[A]](implicitly[CatalystSortableCollection[V]].sortOp(column.untyped, sortAscending = true))(column.uencoder)

  /** Sorts the input array for the given column in descending order, according to
    * the natural ordering of the array elements.
    *
    * apache/spark
    */
  def sortDescending[T, A: Ordering, V[_] : CatalystSortableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, V[A]] =
    new TypedColumn[T, V[A]](implicitly[CatalystSortableCollection[V]].sortOp(column.untyped, sortAscending = false))(column.uencoder)


  /** Creates a new row for each element in the given collection. The column types
    * eligible for this operation are constrained by CatalystExplodableCollection.
    *
    * apache/spark
    */
  def explode[T, A: TypedEncoder, V[_] : CatalystExplodableCollection](column: TypedColumn[T, V[A]]): TypedColumn[T, A] =
    new TypedColumn[T, A](sparkFunctions.explode(column.untyped))
}

trait CatalystSizableCollection[V[_]] {
  def sizeOp(col: Column): Column
}

object CatalystSizableCollection {
  implicit def sizableVector: CatalystSizableCollection[Vector] = new CatalystSizableCollection[Vector] {
    def sizeOp(col: Column): Column = sparkFunctions.size(col)
  }

  implicit def sizableArray: CatalystSizableCollection[Array] = new CatalystSizableCollection[Array] {
    def sizeOp(col: Column): Column = sparkFunctions.size(col)
  }

  implicit def sizableList: CatalystSizableCollection[List] = new CatalystSizableCollection[List] {
    def sizeOp(col: Column): Column = sparkFunctions.size(col)
  }

}

trait CatalystExplodableCollection[V[_]]

object CatalystExplodableCollection {
  implicit def explodableVector: CatalystExplodableCollection[Vector] = new CatalystExplodableCollection[Vector] {}
  implicit def explodableArray: CatalystExplodableCollection[Array] = new CatalystExplodableCollection[Array] {}
  implicit def explodableList: CatalystExplodableCollection[List] = new CatalystExplodableCollection[List] {}
}

trait CatalystSortableCollection[V[_]] {
  def sortOp(col: Column, sortAscending: Boolean): Column
}

object CatalystSortableCollection {
  implicit def sortableVector: CatalystSortableCollection[Vector] = new CatalystSortableCollection[Vector] {
    def sortOp(col: Column, sortAscending: Boolean): Column = sparkFunctions.sort_array(col, sortAscending)
  }

  implicit def sortableArray: CatalystSortableCollection[Array] = new CatalystSortableCollection[Array] {
    def sortOp(col: Column, sortAscending: Boolean): Column = sparkFunctions.sort_array(col, sortAscending)
  }

  implicit def sortableList: CatalystSortableCollection[List] = new CatalystSortableCollection[List] {
    def sortOp(col: Column, sortAscending: Boolean): Column = sparkFunctions.sort_array(col, sortAscending)
  }
}
