package frameless
package ops

import shapeless.ops.hlist.ToTraversable
import shapeless.{HList, SingletonProductArgs}
import syntax._

sealed abstract class DropStrategy(val value: String)

object DropStrategy {
  /** Drops rows only if every specified column is null or NaN for that row. */
  case object All extends DropStrategy("all")

  /** Drops rows containing any null or NaN values. */
  case object Any extends DropStrategy("any")
}

class NaFunctions[T](self: TypedDataset[T])(implicit val encoder: TypedEncoder[T]) {

  /** Returns a new `TypedDataset` that drops rows containing any null or NaN values. */
  def drop(): TypedDataset[T] = self.toDF().na.drop().unsafeTyped

  /** Returns a new `TypedDataset` that drops rows containing less than minNonNulls
    * non-null and non-NaN values.
    */
  def drop(minNonNulls: Int): TypedDataset[T] = self.toDF().na.drop(minNonNulls).unsafeTyped

  /** Returns a new `TypedDataset` that drops rows containing null or NaN values in the
    * specified columns.
    */
  def drop(how: DropStrategy): TypedDataset[T] = self.toDF().na.drop(how.value).unsafeTyped

  /** Returns a new `TypedDataset` that drops rows containing null or NaN values in
    * the provided columns.
    * {{{
    *   ds.na.dropMany('a, 'b)
    * }}}
    */
  object dropMany extends SingletonProductArgs {
    def applyProduct[U <: HList](columns: U)
      (implicit
       i0: ToTraversable.Aux[U, List, Symbol],
       i1: TypedColumn.ExistsAllByName[T, U]
      ): TypedDataset[T] = {

      val names = columns.toList[Symbol].map(_.name)
      val dropped = self.toDF().na.drop(names).as[T](TypedExpressionEncoder[T])

      TypedDataset.create[T](dropped)
    }
  }

}
