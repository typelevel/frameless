package frameless
package ops

import shapeless._

/**
 * A type class to extract the column types out of an HList of [[frameless.TypedColumn]].
 *
 * @note This type class is mostly a workaround to issue with slow implicit derivation for Comapped.
 * @example
 * {{{
 *   type U = TypedColumn[T,A] :: TypedColumn[T,B] :: TypedColumn[T,C] :: HNil
 *   type Out = A :: B :: C :: HNil
 * }}}
 */
trait ColumnTypes[T, U <: HList] {
  type Out <: HList
}

object ColumnTypes {
  type Aux[T, U <: HList, Out0 <: HList] = ColumnTypes[T, U] { type Out = Out0 }

  implicit def deriveHNil[T]: ColumnTypes.Aux[T, HNil, HNil] = new ColumnTypes[T, HNil] {
    type Out = HNil
  }

  implicit def deriveCons[T, H, TT <: HList, V <: HList](
      implicit tail: ColumnTypes.Aux[T, TT, V]
  ): ColumnTypes.Aux[T, TypedColumn[T, H] :: TT, H :: V] =
    new ColumnTypes[T, TypedColumn[T, H] :: TT] { type Out = H :: V }
}
