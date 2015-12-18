package typedframe

import shapeless._
import shapeless.ops.hlist._
import shapeless.ops.record.Remover
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

// https://github.com/milessabin/shapeless/pull/503

/** Typeclass witnessing that all the elements of an HList have instances of the given typeclass. */
sealed trait LiftAll[F[_], In <: HList] {
  type Out <: HList
  def instances: Out
}

object LiftAll {
  type Aux[F[_], In0 <: HList, Out0 <: HList] = LiftAll[F, In0] {type Out = Out0}
  class Curried[F[_]] {def apply[In <: HList](in: In)(implicit ev: LiftAll[F, In]) = ev}
  def apply[F[_]] = new Curried[F]
  def apply[F[_], In <: HList](implicit ev: LiftAll[F, In]) = ev
  implicit def hnil[F[_]]: LiftAll.Aux[F, HNil, HNil] = new LiftAll[F, HNil] {
    type Out = HNil
    def instances = HNil
  }
  implicit def hcons[F[_], H, T <: HList]
    (implicit headInstance: F[H], tailInstances: LiftAll[F, T]): Aux[F, H :: T, F[H] :: tailInstances.Out] =
      new LiftAll[F, H :: T] {
        type Out = F[H] :: tailInstances.Out
        def instances = headInstance :: tailInstances.instances
  }
}

/** Type class supporting conversion of this `HList` to a tuple, up to Tuple64. */
trait ManyTupler[L <: HList] extends DepFn1[L] with Serializable

object ManyTupler extends ManyTuplerInstances {
  def apply[L <: HList](implicit tupler: ManyTupler[L]): Aux[L, tupler.Out] = tupler
}
