package frameless.ops

import frameless.{CatalystOrdered, SortedTypedColumn}
import shapeless.{::, HList, HNil}

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Either one of the selected columns is not sortable (${U}), " +
    "or no ordering (ascending/descending) has been selected. " +
    "Select an ordering on any column (t) using the t.asc or t.desc methods.")
trait SortableColumnTypes[T, U <: HList]

object SortableColumnTypes {
  implicit def deriveHNil[T]: SortableColumnTypes[T, HNil] =
    new SortableColumnTypes[T, HNil] { type Out = HNil }

  implicit def deriveCons[T, H, TT <: HList, V <: HList]
  (implicit
   order: CatalystOrdered[H],
   tail: SortableColumnTypes[T, TT]
  ): SortableColumnTypes[T, SortedTypedColumn[T, H] :: TT] =
    new SortableColumnTypes[T, SortedTypedColumn[T, H] :: TT] {}
}
