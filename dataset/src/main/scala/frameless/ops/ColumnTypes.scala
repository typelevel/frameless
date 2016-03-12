package frameless
package ops

import shapeless._

trait ColumnTypes[V, U <: HList] {
  type Out <: HList
}

object ColumnTypes {
  type Aux[V, U <: HList, Out0 <: HList] = ColumnTypes[V, U] {type Out = Out0}

  implicit def deriveHNil[T]: ColumnTypes.Aux[T, HNil, HNil] = new ColumnTypes[T, HNil] { type Out = HNil }

  implicit def deriveCons[V, H, TT <: HList, T <: HList](
    implicit tail: ColumnTypes.Aux[V, TT, T]
  ): ColumnTypes.Aux[V, TypedColumn[V, H] :: TT, H :: T] =
    new ColumnTypes[V, TypedColumn[V, H] :: TT] {type Out = H :: T}
}
