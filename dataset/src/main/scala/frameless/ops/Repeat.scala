package frameless
package ops

import shapeless.{HList, Nat, Succ}
import shapeless.ops.hlist.Prepend

/* By Jeremy Smith */
trait Repeat[L <: HList, N <: Nat] {
  type Out <: HList
}

object Repeat {
  type Aux[L <: HList, N <: Nat, Out0 <: HList] = Repeat[L, N] { type Out = Out0 }

  implicit def base[L <: HList]: Aux[L, Nat._1, L] = new Repeat[L, Nat._1] {
    type Out = L
  }

  implicit def succ[L <: HList, Prev <: Nat, PrevOut <: HList, P <: HList]
  (implicit
   prev: Aux[L, Prev, PrevOut],
   prepend: Prepend.Aux[L, PrevOut, P]
  ): Aux[L, Succ[Prev], P] = new Repeat[L, Succ[Prev]] {
    type Out = P
  }
}