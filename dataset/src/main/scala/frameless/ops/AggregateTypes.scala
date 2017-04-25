package frameless
package ops

import shapeless._

trait AggregateTypes[V, U <: HList] {
  type Out <: HList
}

object AggregateTypes {
  type Aux[V, U <: HList, Out0 <: HList] = AggregateTypes[V, U] {type Out = Out0}

  implicit def deriveHNil[T]: AggregateTypes.Aux[T, HNil, HNil] = new AggregateTypes[T, HNil] { type Out = HNil }

  implicit def deriveCons1[V, H, TT <: HList, T <: HList](
    implicit tail: AggregateTypes.Aux[V, TT, T]
  ): AggregateTypes.Aux[V, TypedAggregate[V, H] :: TT, H :: T] =
    new AggregateTypes[V, TypedAggregate[V, H] :: TT] {type Out = H :: T}
}
