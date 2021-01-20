package frameless
package ml
package internals

import org.apache.spark.ml.linalg._
import shapeless.ops.hlist.Length
import shapeless.{HList, LabelledGeneric, Nat, Witness}

import scala.annotation.implicitNotFound

/**
  * Can be used for linear reg algorithm
  */
@implicitNotFound(
  msg = "Cannot prove that ${Inputs} is a valid input type. " +
    "Input type must only contain a field of type Double (the label) and a field of type " +
    "org.apache.spark.ml.linalg.Vector (the features) and optional field of float type (weight)."
)
trait LinearInputsChecker[Inputs] {
  val featuresCol: String
  val labelCol: String
  val weightCol: Option[String]
}

object LinearInputsChecker {

  implicit def checkLinearInputs[
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
  ): LinearInputsChecker[Inputs] = {
    new LinearInputsChecker[Inputs] {
      val labelCol: String = implicitly[Witness.Aux[LabelK]].value.name
      val featuresCol: String = implicitly[Witness.Aux[FeaturesK]].value.name
      val weightCol: Option[String] = None
    }
  }

  implicit def checkLinearInputs2[
  Inputs,
  InputsRec <: HList,
  LabelK <: Symbol,
  FeaturesK <: Symbol,
  WeightK <: Symbol](
    implicit
    i0: LabelledGeneric.Aux[Inputs, InputsRec],
    i1: Length.Aux[InputsRec, Nat._3],
    i2: SelectorByValue.Aux[InputsRec, Vector, FeaturesK],
    i3: Witness.Aux[FeaturesK],
    i4: SelectorByValue.Aux[InputsRec, Double, LabelK],
    i5: Witness.Aux[LabelK],
    i6: SelectorByValue.Aux[InputsRec, Float, WeightK],
    i7: Witness.Aux[WeightK]
  ): LinearInputsChecker[Inputs] = {
    new LinearInputsChecker[Inputs] {
      val labelCol: String = implicitly[Witness.Aux[LabelK]].value.name
      val featuresCol: String = implicitly[Witness.Aux[FeaturesK]].value.name
      val weightCol: Option[String] = Some(implicitly[Witness.Aux[WeightK]].value.name)
    }
  }

}
