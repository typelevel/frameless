package frameless
package ops

import shapeless._

/** A type class to extract the column types out of an HList of [[frameless.TypedColumn]].
  *
  * @note This type class is mostly a workaround to issue with slow implicit derivation for Comapped.
  * @example
  * {{{
  *   type U = TypedColumn[A] :: TypedColumn[B] :: TypedColumn[C] :: HNil
  *   type Out = A :: B :: C :: HNil
  * }}}
  */
trait ColumnTypes[U <: HList] {
  type Out <: HList
}

object ColumnTypes {
  type Aux[U <: HList, Out0 <: HList] = ColumnTypes[U] {type Out = Out0}

  implicit def deriveHNil: ColumnTypes.Aux[HNil, HNil] = new ColumnTypes[HNil] { type Out = HNil }

  implicit def deriveCons[H, TT <: HList, V <: HList](
    implicit tail: ColumnTypes.Aux[TT, V]
  ): ColumnTypes.Aux[TypedColumn[H] :: TT, H :: V] =
    new ColumnTypes[TypedColumn[H] :: TT] {type Out = H :: V}
}
