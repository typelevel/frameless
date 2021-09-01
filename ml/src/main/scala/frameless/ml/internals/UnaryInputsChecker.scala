package frameless
package ml
package internals

import scala.annotation.implicitNotFound

import shapeless.{HList, LabelledGeneric, Nat, Witness}
import shapeless.ops.hlist.Length

/**
 * Can be used for all unary transformers (i.e almost all of them)
 */
@implicitNotFound(
  msg =
    "Cannot prove that ${Inputs} is a valid input type. Input type must have only one field of type ${Expected}"
)
trait UnaryInputsChecker[Inputs, Expected] {
  val inputCol: String
}

object UnaryInputsChecker {

  implicit def checkUnaryInputs[Inputs, Expected, InputsRec <: HList, InputK <: Symbol](
      implicit i0: LabelledGeneric.Aux[Inputs, InputsRec],
      i1: Length.Aux[InputsRec, Nat._1],
      i2: SelectorByValue.Aux[InputsRec, Expected, InputK],
      i3: Witness.Aux[InputK]
  ): UnaryInputsChecker[Inputs, Expected] = new UnaryInputsChecker[Inputs, Expected] {
    val inputCol: String = implicitly[Witness.Aux[InputK]].value.name
  }

}
