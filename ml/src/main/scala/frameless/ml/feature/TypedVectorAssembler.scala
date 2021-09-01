package frameless
package ml
package feature

import scala.annotation.implicitNotFound

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector

import shapeless.{HList, HNil, LabelledGeneric, _}
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.{Keys, Values}

/**
 * A feature transformer that merges multiple columns into a vector column.
 */
final class TypedVectorAssembler[Inputs] private[ml] (
    vectorAssembler: VectorAssembler,
    inputCols: Array[String])
    extends AppendTransformer[Inputs, TypedVectorAssembler.Output, VectorAssembler] {

  val transformer: VectorAssembler =
    vectorAssembler.setInputCols(inputCols).setOutputCol(AppendTransformer.tempColumnName)

}

object TypedVectorAssembler {
  case class Output(vector: Vector)

  def apply[Inputs](implicit inputsChecker: TypedVectorAssemblerInputsChecker[Inputs])
      : TypedVectorAssembler[Inputs] = {
    new TypedVectorAssembler(new VectorAssembler(), inputsChecker.inputCols.toArray)
  }
}

@implicitNotFound(
  msg =
    "Cannot prove that ${Inputs} is a valid input type. Input type must only contain fields of numeric or boolean types."
)
private[ml] trait TypedVectorAssemblerInputsChecker[Inputs] {
  val inputCols: Seq[String]
}

private[ml] object TypedVectorAssemblerInputsChecker {
  implicit def checkInputs[
      Inputs,
      InputsRec <: HList,
      InputsKeys <: HList,
      InputsVals <: HList](
      implicit inputsGen: LabelledGeneric.Aux[Inputs, InputsRec],
      inputsKeys: Keys.Aux[InputsRec, InputsKeys],
      inputsKeysTraverse: ToTraversable.Aux[InputsKeys, Seq, Symbol],
      inputsValues: Values.Aux[InputsRec, InputsVals],
      inputsTypeCheck: TypedVectorAssemblerInputsValueChecker[InputsVals]
  ): TypedVectorAssemblerInputsChecker[Inputs] = new TypedVectorAssemblerInputsChecker[Inputs] {
    val inputCols: Seq[String] = inputsKeys.apply.to[Seq].map(_.name)
  }
}

private[ml] trait TypedVectorAssemblerInputsValueChecker[InputsVals]

private[ml] object TypedVectorAssemblerInputsValueChecker {
  implicit def hnilCheckInputsValue: TypedVectorAssemblerInputsValueChecker[HNil] =
    new TypedVectorAssemblerInputsValueChecker[HNil] {}

  implicit def hlistCheckInputsValueNumeric[H, T <: HList](
      implicit ch: CatalystNumeric[H],
      tt: TypedVectorAssemblerInputsValueChecker[T]
  ): TypedVectorAssemblerInputsValueChecker[H :: T] =
    new TypedVectorAssemblerInputsValueChecker[H :: T] {}

  implicit def hlistCheckInputsValueBoolean[T <: HList](
      implicit tt: TypedVectorAssemblerInputsValueChecker[T]
  ): TypedVectorAssemblerInputsValueChecker[Boolean :: T] =
    new TypedVectorAssemblerInputsValueChecker[Boolean :: T] {}
}
