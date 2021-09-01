package frameless
package ops

import shapeless.{HList, Nat, Succ}
import shapeless.ops.hlist.Prepend

/**
 * Typeclass supporting repeating L-typed HLists N times.
 *
 * Repeat[Int :: String :: HNil, Nat._2].Out =:= Int :: String :: Int :: String :: HNil
 *
 * By Jeremy Smith. To be replaced by `shapeless.ops.hlists.Repeat` once
 * (https://github.com/milessabin/shapeless/pull/730 is published.
 */
trait Repeat[L <: HList, N <: Nat] {
  type Out <: HList
}

object Repeat {
  type Aux[L <: HList, N <: Nat, Out0 <: HList] = Repeat[L, N] { type Out = Out0 }

  implicit def base[L <: HList]: Aux[L, Nat._1, L] = new Repeat[L, Nat._1] {
    type Out = L
  }

  implicit def succ[L <: HList, Prev <: Nat, PrevOut <: HList, P <: HList](
      implicit i0: Aux[L, Prev, PrevOut],
      i1: Prepend.Aux[L, PrevOut, P]): Aux[L, Succ[Prev], P] = new Repeat[L, Succ[Prev]] {
    type Out = P
  }
}
