package frameless

import shapeless._
import shapeless.ops.record.Remover

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
  implicit def apply[T]: IsXLTuple[T] = macro IsXLTupleMacro.mk[T]
}
