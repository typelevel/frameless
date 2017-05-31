package frameless
package ops

import shapeless._

/** A type class to extract the column types out of an HList of [[frameless.TypedColumn]].
  *
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
  type Aux[T, U <: HList, Out0 <: HList] = ColumnTypes[T, U] {type Out = Out0}

  implicit def deriveHNil[T]: ColumnTypes.Aux[T, HNil, HNil] = new ColumnTypes[T, HNil] { type Out = HNil }

  implicit def deriveCons[T, Head, Tail <: HList, ColTypes <: HList](
    implicit tail: ColumnTypes.Aux[T, Tail, ColTypes]
  ): ColumnTypes.Aux[T, TypedColumn[T, Head] :: Tail, Head :: ColTypes] =
    new ColumnTypes[T, TypedColumn[T, Head] :: Tail] {type Out = Head :: ColTypes}
}
