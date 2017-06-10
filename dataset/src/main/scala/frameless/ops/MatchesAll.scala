package frameless
package ops

import shapeless.HList
import shapeless.ops.hlist.Filter

import scala.annotation.implicitNotFound


@implicitNotFound("HList of type '${L}' is NOT comprised of only elements of type '${U}'")
trait MatchesAll[L <: HList, U]

object MatchesAll {
  implicit def matchesAll[L <: HList, U, Rest <: HList]
  (implicit
   filtered: Filter.Aux[L, U, Rest],
   remainedSame: Rest =:= L
  ): MatchesAll[L, U] = new MatchesAll[L, U] {}
}