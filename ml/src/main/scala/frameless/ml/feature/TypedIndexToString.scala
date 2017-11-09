package frameless
package ml
package feature

import frameless.ml.internals.SelectorByValue
import org.apache.spark.ml.feature.IndexToString
import shapeless.{HList, LabelledGeneric, Witness}
import scala.annotation.implicitNotFound
import shapeless._
import shapeless.ops.hlist.Length

final class TypedIndexToString[Inputs] private[ml](indexToString: IndexToString, inputCol: String)
  extends AppendTransformer[Inputs, TypedIndexToString.Outputs, IndexToString] {

  val transformer: IndexToString =
    indexToString
      .setInputCol(inputCol)
      .setOutputCol(AppendTransformer.tempColumnName)

}

object TypedIndexToString {
  case class Outputs(originalOutput: String)

  def create[Inputs](labels: Array[String])
                    (implicit inputsChecker: TypedIndexToStringInputsChecker[Inputs]): TypedIndexToString[Inputs] = {
    new TypedIndexToString[Inputs](new IndexToString().setLabels(labels), inputsChecker.inputCol)
  }
}

@implicitNotFound(
  msg = "Cannot prove that ${Inputs} is a valid input type for TypedIndexToString. " +
    "Input type must only contain a field of type Double (index)"
)
private[ml] trait TypedIndexToStringInputsChecker[Inputs] {
  val inputCol: String
}

private[ml] object TypedIndexToStringInputsChecker {
  implicit def checkInputs[
  Inputs,
  InputsRec <: HList,
  InputK <: Symbol](
   implicit
   inputsGen: LabelledGeneric.Aux[Inputs, InputsRec],
   sizeCheck: Length.Aux[InputsRec, Nat._1],
   labelSelect: SelectorByValue.Aux[InputsRec, Double, InputK],
   inputW: Witness.Aux[InputK]
  ): TypedIndexToStringInputsChecker[Inputs] = new TypedIndexToStringInputsChecker[Inputs] {
    val inputCol: String = inputW.value.name
  }
}
