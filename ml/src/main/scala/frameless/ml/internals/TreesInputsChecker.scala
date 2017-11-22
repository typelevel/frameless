package frameless.ml.internals

import shapeless.ops.hlist.Length
import shapeless.{HList, LabelledGeneric, Nat, Witness}
import org.apache.spark.ml.linalg._

import scala.annotation.implicitNotFound

/**
  * Can be used for all tree-based ML algorithm (decision tree, random forest, gradient-boosted trees)
  */
@implicitNotFound(
  msg = "Cannot prove that ${Inputs} is a valid input type." +
    "Input type must only contain a field of type Double (label) and a field of type " +
    "org.apache.spark.ml.linalg.Vector (features)."
)
trait TreesInputsChecker[Inputs] {
  val featuresCol: String
  val labelCol: String
}

object TreesInputsChecker {

  implicit def checkTreesInputs[
  Inputs,
  InputsRec <: HList,
  LabelK <: Symbol,
  FeaturesK <: Symbol](
    implicit
    i0: LabelledGeneric.Aux[Inputs, InputsRec],
    i1: Length.Aux[InputsRec, Nat._2],
    i2: SelectorByValue.Aux[InputsRec, Double, LabelK],
    i3: Witness.Aux[LabelK],
    i4: SelectorByValue.Aux[InputsRec, Vector, FeaturesK],
    i5: Witness.Aux[FeaturesK]
  ): TreesInputsChecker[Inputs] = {
    new TreesInputsChecker[Inputs] {
      val labelCol: String = implicitly[Witness.Aux[LabelK]].value.name
      val featuresCol: String = implicitly[Witness.Aux[FeaturesK]].value.name
    }
  }

}
