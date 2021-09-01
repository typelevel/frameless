package frameless
package ml
package internals

import scala.annotation.implicitNotFound

import org.apache.spark.ml.linalg.Vector

import shapeless.{HList, LabelledGeneric, Nat, Witness}
import shapeless.ops.hlist.Length

/**
 * Can be used whenever algorithm requires only vector
 */
@implicitNotFound(
  msg = "Cannot prove that ${Inputs} is a valid input type. " +
    "Input type must only contain a field of type org.apache.spark.ml.linalg.Vector (the features)."
)
trait VectorInputsChecker[Inputs] {
  val featuresCol: String
}

object VectorInputsChecker {
  implicit def checkVectorInput[Inputs, InputsRec <: HList, FeaturesK <: Symbol](
      implicit i0: LabelledGeneric.Aux[Inputs, InputsRec],
      i1: Length.Aux[InputsRec, Nat._1],
      i2: SelectorByValue.Aux[InputsRec, Vector, FeaturesK],
      i3: Witness.Aux[FeaturesK]
  ): VectorInputsChecker[Inputs] = {
    new VectorInputsChecker[Inputs] {
      val featuresCol: String = i3.value.name
    }
  }
}
