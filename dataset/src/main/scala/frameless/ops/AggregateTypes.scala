package frameless
package ops

import shapeless._

/**
 * A type class to extract the column types out of an HList of [[frameless.TypedAggregate]].
 *
 * @note
 *   This type class is mostly a workaround to issue with slow implicit derivation for Comapped.
 * @example
 * {{{
 *   type U = TypedAggregate[T,A] :: TypedAggregate[T,B] :: TypedAggregate[T,C] :: HNil
 *   type Out = A :: B :: C :: HNil
 * }}}
 */
trait AggregateTypes[V, U <: HList] {
  type Out <: HList
}

object AggregateTypes {
  type Aux[V, U <: HList, Out0 <: HList] = AggregateTypes[V, U] { type Out = Out0 }

  implicit def deriveHNil[T]: AggregateTypes.Aux[T, HNil, HNil] = new AggregateTypes[T, HNil] {
    type Out = HNil
  }

  implicit def deriveCons1[T, H, TT <: HList, V <: HList](
      implicit tail: AggregateTypes.Aux[T, TT, V]
  ): AggregateTypes.Aux[T, TypedAggregate[T, H] :: TT, H :: V] =
    new AggregateTypes[T, TypedAggregate[T, H] :: TT] { type Out = H :: V }
}
