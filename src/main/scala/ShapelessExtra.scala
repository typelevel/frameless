package typedframe

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

/** Type class supporting type safe cast.
  * Differs from shapeless.Typeable for it's null support.*/
trait NullTypeable[T] extends Serializable {
  def cast(t: Any): Option[T]
}

object NullTypeable {
  def apply[T](implicit t: NullTypeable[T]): NullTypeable[T] = t
  
  implicit def nullTypeableFromTypeable[T](implicit typeable: Typeable[T]): NullTypeable[T] =
    new NullTypeable[T] {
      def cast(t: Any): Option[T] =
        if(t == null) Some(null.asInstanceOf[T]) else typeable.cast(t)
    }
}

/** Type class supporting type safe conversion of `Traversables` to `HLists`.
  * Differs from shapeless.ops.traversable.FromTraversable for it's null support. */
trait FromTraversableNullable[Out <: HList] extends Serializable {
  def apply(l: GenTraversable[_]): Option[Out]
}

object FromTraversableNullable {
  def apply[Out <: HList](implicit from: FromTraversableNullable[Out]) = from
  
  implicit def hnilFromTraversableNullable[T]: FromTraversableNullable[HNil] =
    new FromTraversableNullable[HNil] {
      def apply(l: GenTraversable[_]) =
        if(l.isEmpty) Some(HNil) else None 
    }
  
  implicit def hlistFromTraversableNullable[OutH, OutT <: HList]
    (implicit
      flt: FromTraversableNullable[OutT],
      oc: NullTypeable[OutH]
    ): FromTraversableNullable[OutH :: OutT] =
      new FromTraversableNullable[OutH :: OutT] {
        def apply(l: GenTraversable[_]): Option[OutH :: OutT] =
          if(l.isEmpty) None else for(h <- oc.cast(l.head); t <- flt(l.tail)) yield h :: t
      }
}

/** Type class supporting conversion of this `HList` to a tuple, up to Tuple64. */
trait XLTupler[L <: HList] extends DepFn1[L] with Serializable

object XLTupler extends XLTuplerInstances {
  def apply[L <: HList](implicit tupler: XLTupler[L]): Aux[L, tupler.Out] = tupler
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
trait Fille[N, A] extends DepFn1[A] with Serializable { type Out <: HList }

object Fille {
  def apply[N, A](implicit fille: Fille[N, A]): Aux[N, A, fille.Out] = fille

  type Aux[N, A, Out0] = Fille[N, A] { type Out = Out0 }

  implicit def fille1Zero[A]: Aux[Nat._0, A, HNil] =
    new Fille[Nat._0, A] {
      type Out = HNil
      def apply(elem: A) = HNil
    }

  implicit def fille1Succ[N <: Nat, A, OutT <: HList]
    (implicit prev: Aux[N, A, OutT]): Aux[Succ[N], A, A :: OutT] =
      new Fille[Succ[N], A] {
        type Out = A :: OutT
        def apply(elem: A) = elem :: prev(elem)
      }

  implicit def fille2[A, N1 <: Nat, N2 <: Nat, SubOut, OutT <: HList]
    (implicit subFille: Aux[N2, A, SubOut], fille: Aux[N1, SubOut, OutT]): Aux[(N1, N2), A, OutT] =
      new Fille[(N1, N2), A] {
        type Out = OutT
        def apply(elem: A) = fille(subFille(elem))
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
