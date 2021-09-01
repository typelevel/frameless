package frameless
package ml
package internals

import shapeless.labelled.FieldType
import shapeless.{::, DepFn1, HList, Witness}

/**
 * Typeclass supporting record selection by value type (returning the first key whose value is of type `Value`)
 */
trait SelectorByValue[L <: HList, Value] extends DepFn1[L] with Serializable {
  type Out <: Symbol
}

object SelectorByValue {
  type Aux[L <: HList, Value, Out0 <: Symbol] = SelectorByValue[L, Value] { type Out = Out0 }

  implicit def select[K <: Symbol, T <: HList, Value](
      implicit wk: Witness.Aux[K]): Aux[FieldType[K, Value] :: T, Value, K] = {
    new SelectorByValue[FieldType[K, Value] :: T, Value] {
      type Out = K
      def apply(l: FieldType[K, Value] :: T): Out = wk.value
    }
  }

  implicit def recurse[H, T <: HList, Value](
      implicit st: SelectorByValue[T, Value]): Aux[H :: T, Value, st.Out] = {
    new SelectorByValue[H :: T, Value] {
      type Out = st.Out
      def apply(l: H :: T): Out = st(l.tail)
    }
  }
}
