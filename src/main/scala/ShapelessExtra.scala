import shapeless._
import shapeless.ops.hlist._

// https://github.com/milessabin/shapeless/pull/502

/**
 * Type class supporting `HList` union. In case of duplicate types, this operation is a order-preserving multi-set union.
 * If type `T` appears n times in this `HList` and m > n times in `M`, the resulting `HList` contains the first n elements
 * of type `T` in this `HList`, followed by the last m - n element of type `T` in `M`.
 * 
 * @author Olivier Blanvillain
 */
trait Union[L <: HList, M <: HList] extends DepFn2[L, M] with Serializable { type Out <: HList }

trait LowPriorityUnion {
  type Aux[L <: HList, M <: HList, Out0 <: HList] = Union[L, M] { type Out = Out0 }

  implicit def hlistUnion1[H, T <: HList, M <: HList]
    (implicit u: Union[T, M]): Aux[H :: T, M, H :: u.Out] =
      new Union[H :: T, M] {
        type Out = H :: u.Out
        def apply(l: H :: T, m: M): Out = l.head :: u(l.tail, m)
      }
}

object Union extends LowPriorityUnion {
  def apply[L <: HList, M <: HList](implicit union: Union[L, M]): Aux[L, M, union.Out] = union

  implicit def hlistUnion[M <: HList]: Aux[HNil, M, M] =
    new Union[HNil, M] {
      type Out = M
      def apply(l: HNil, m: M): Out = m
    }

  implicit def hlistUnion2[H, T <: HList, M <: HList, MR <: HList]
    (implicit
      r: Remove.Aux[M, H, (H, MR)],
      u: Union[T, MR]
    ): Aux[H :: T, M, H :: u.Out] =
      new Union[H :: T, M] {
        type Out = H :: u.Out
        def apply(l: H :: T, m: M): Out = l.head :: u(l.tail, r(m)._2)
      }
}

/**
 * Type class supporting `HList` intersection. In case of duplicate types, this operation is a multiset intersection.
 * If type `T` appears n times in this `HList` and m < n times in `M`, the resulting `HList` contains the first m
 * elements of type `T` in this `HList`.
 *
 * Also available if `M` contains types absent in this `HList`.
 *
 * @author Olivier Blanvillain
 */
trait Intersection[L <: HList, M <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

trait LowPriorityIntersection {
  type Aux[L <: HList, M <: HList, Out0 <: HList] = Intersection[L, M] { type Out = Out0 }

  implicit def hlistIntersection1[H, T <: HList, M <: HList]
    (implicit i: Intersection[T, M]): Aux[H :: T, M, i.Out] =
      new Intersection[H :: T, M] {
        type Out = i.Out
        def apply(l: H :: T): Out = i(l.tail)
      }
}

object Intersection extends LowPriorityIntersection {
  def apply[L <: HList, M <: HList](implicit intersection: Intersection[L, M]): Aux[L, M, intersection.Out] = intersection

  implicit def hnilIntersection[M <: HList]: Aux[HNil, M, HNil] =
    new Intersection[HNil, M] {
      type Out = HNil
      def apply(l: HNil): Out = HNil
    }

  implicit def hlistIntersection2[H, T <: HList, M <: HList, MR <: HList]
    (implicit
      r: Remove.Aux[M, H, (H, MR)],
      i: Intersection[T, MR]
    ): Aux[H :: T, M, H :: i.Out] =
      new Intersection[H :: T, M] {
        type Out = H :: i.Out
        def apply(l: H :: T): Out = l.head :: i(l.tail)
      }
}

/**
 * Type class supporting `HList` subtraction. In case of duplicate types, this operation is a multiset difference.
 * If type `T` appears n times in this `HList` and m < n times in `M`, the resulting `HList` contains the last n - m
 * elements of type `T` in this `HList`.
 *
 * Also available if `M` contains types absent in this `HList`.
 *
 * @author Olivier Blanvillain
 */
trait Diff[L <: HList, M <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

trait LowPriorityDiff {
  type Aux[L <: HList, M <: HList, Out0] = Diff[L, M] { type Out = Out0 }

  implicit def hconsDiff1[L <: HList, H, T <: HList]
    (implicit d: Diff[L, T]): Aux[L, H :: T, d.Out] =
      new Diff[L, H :: T] {
        type Out = d.Out
        def apply(l: L): Out = d(l)
      }
}

object Diff extends LowPriorityDiff {
  def apply[L <: HList, M <: HList](implicit diff: Diff[L, M]): Aux[L, M, diff.Out] = diff

  implicit def hnilDiff[L <: HList]: Aux[L, HNil, L] =
    new Diff[L, HNil] {
      type Out = L
      def apply(l: L): Out = l
    }

  implicit def hconsDiff2[L <: HList, LT <: HList, H, T <: HList]
    (implicit
      r: Remove.Aux[L, H, (H, LT)],
      d: Diff[LT, T]
    ): Aux[L, H :: T, d.Out] =
      new Diff[L, H :: T] {
        type Out = d.Out
        def apply(l: L): Out = d(r(l)._2)
      }
}
