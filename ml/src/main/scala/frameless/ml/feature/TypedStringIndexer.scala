package frameless
package ml
package feature

import frameless.ml.feature.TypedStringIndexer.HandleInvalid
import frameless.ml.internals.SelectorByValue
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import shapeless.ops.hlist.Length
import shapeless.{HList, LabelledGeneric, Nat, Witness}
import scala.annotation.implicitNotFound

final class TypedStringIndexer[Inputs] private[ml](stringIndexer: StringIndexer, inputCol: String)
  extends TypedEstimator[Inputs, TypedStringIndexer.Outputs, StringIndexerModel] {

  val estimator: StringIndexer = stringIndexer
    .setInputCol(inputCol)
    .setOutputCol(AppendTransformer.tempColumnName)

  def setHandleInvalid(value: HandleInvalid): TypedStringIndexer[Inputs] = copy(stringIndexer.setHandleInvalid(value.sparkValue))

  private def copy(newStringIndexer: StringIndexer): TypedStringIndexer[Inputs] =
    new TypedStringIndexer[Inputs](newStringIndexer, inputCol)
}

object TypedStringIndexer {
  case class Outputs(indexedOutput: Double)

  sealed trait HandleInvalid {
    val sparkValue: String
  }
  object HandleInvalid {
    case object Error extends HandleInvalid {
      val sparkValue = "error"
    }
    case object Skip extends HandleInvalid {
      val sparkValue = "skip"
    }
    case object Keep extends HandleInvalid {
      val sparkValue = "keep"
    }
  }

  def create[Inputs]()
                    (implicit inputsChecker: TypedStringIndexerInputsChecker[Inputs]): TypedStringIndexer[Inputs] = {
    new TypedStringIndexer[Inputs](new StringIndexer(), inputsChecker.inputCol)
  }
}

@implicitNotFound(
  msg = "Cannot prove that ${Inputs} is a valid input type for TypedStringIndexer. " +
    "Input type must only contain a field of type String (string to index)"
)
private[ml] trait TypedStringIndexerInputsChecker[Inputs] {
  val inputCol: String
}

private[ml] object TypedStringIndexerInputsChecker {
  implicit def checkInputs[
  Inputs,
  InputsRec <: HList,
  InputK <: Symbol](
    implicit
    inputsGen: LabelledGeneric.Aux[Inputs, InputsRec],
    sizeCheck: Length.Aux[InputsRec, Nat._1],
    labelSelect: SelectorByValue.Aux[InputsRec, String, InputK],
    inputW: Witness.Aux[InputK]
  ): TypedStringIndexerInputsChecker[Inputs] = new TypedStringIndexerInputsChecker[Inputs] {
    val inputCol: String = inputW.value.name
  }
}
