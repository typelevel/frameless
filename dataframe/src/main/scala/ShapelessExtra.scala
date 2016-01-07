package frameless

import shapeless._
import shapeless.ops.record.Remover
import scala.collection.GenTraversable
import scala.language.higherKinds

/** Type class supporting multiple record field removal. */
@annotation.implicitNotFound(msg = "No fields ${K} in record ${L}")
trait AllRemover[L <: HList, K <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object AllRemover {
  def apply[L <: HList, K <: HList](implicit rf: AllRemover[L, K]): Aux[L, K, rf.Out] = rf
  
  type Aux[L <: HList, K <: HList, Out0 <: HList] = AllRemover[L, K] { type Out = Out0 }
  
  implicit def hnilAllRemover[L <: HList]: Aux[L, HNil, L] =
    new AllRemover[L, HNil] {
      type Out = L
      def apply(l: L): Out = l
    }
  
  implicit def hconsAllRemover[L <: HList, H, T <: HList, V, R <: HList]
    (implicit
      r: Remover.Aux[L, H, (V, R)],
      i: AllRemover[R, T]
    ): Aux[L, H :: T, i.Out] =
      new AllRemover[L, H :: T] {
        type Out = i.Out
        def apply(l: L): Out = i(r(l)._2)
      }
}

/** Type class witnessing that a type a tuple, up to Tuple64. */
trait IsXLTuple[T]

object IsXLTuple {
  import scala.language.experimental.macros
  import scala.reflect.macros.whitebox
  
  implicit def apply[T]: IsXLTuple[T] = macro IsXLTupleMacro.mk[T]
}

/** Type class supporting producing a HList of shape `N` filled with elements of type `A`.
  * (To be removed when https://github.com/milessabin/shapeless/pull/525 is published)*/
trait Fill[N, A] extends DepFn1[A] with Serializable { type Out <: HList }

object Fill {
  def apply[N, A](implicit fille: Fill[N, A]): Aux[N, A, fille.Out] = fille

  type Aux[N, A, Out0] = Fill[N, A] { type Out = Out0 }

  implicit def fill1Zero[A]: Aux[Nat._0, A, HNil] =
    new Fill[Nat._0, A] {
      type Out = HNil
      def apply(elem: A) = HNil
    }

  implicit def fill1Succ[N <: Nat, A, OutT <: HList]
    (implicit prev: Aux[N, A, OutT]): Aux[Succ[N], A, A :: OutT] =
      new Fill[Succ[N], A] {
        type Out = A :: OutT
        def apply(elem: A) = elem :: prev(elem)
      }

  implicit def fill2[A, N1 <: Nat, N2 <: Nat, SubOut, OutT <: HList]
    (implicit subFill: Aux[N2, A, SubOut], fille: Aux[N1, SubOut, OutT]): Aux[(N1, N2), A, OutT] =
      new Fill[(N1, N2), A] {
        type Out = OutT
        def apply(elem: A) = fille(subFill(elem))
      }
}

/** Type class witnessing that all the elements of an `HList` have instances of the given typeclass.
  * (To be removed when https://github.com/milessabin/shapeless/pull/503 is published) */
sealed trait LiftAll[F[_], In <: HList] {
  type Out <: HList
  def instances: Out
}

object LiftAll {
  type Aux[F[_], In0 <: HList, Out0 <: HList] = LiftAll[F, In0] { type Out = Out0 }
  
  class Curried[F[_]] {
    def apply[In <: HList](in: In)(implicit ev: LiftAll[F, In]) = ev
  }
  def apply[F[_]] = new Curried[F]
  def apply[F[_], In <: HList](implicit ev: LiftAll[F, In]) = ev
  
  implicit def hnil[F[_]]: LiftAll.Aux[F, HNil, HNil] = new LiftAll[F, HNil] {
    type Out = HNil
    def instances = HNil
  }
  
  implicit def hcons[F[_], H, T <: HList]
    (implicit
      headInstance: F[H],
      tailInstances: LiftAll[F, T]
    ): Aux[F, H :: T, F[H] :: tailInstances.Out] =
      new LiftAll[F, H :: T] {
        type Out = F[H] :: tailInstances.Out
        def instances = headInstance :: tailInstances.instances
      }
}
