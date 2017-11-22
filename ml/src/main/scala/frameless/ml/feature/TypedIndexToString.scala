package frameless
package ml
package feature

import frameless.ml.internals.UnaryInputsChecker
import org.apache.spark.ml.feature.IndexToString

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
                    (implicit inputsChecker: UnaryInputsChecker[Inputs, Double]): TypedIndexToString[Inputs] = {
    new TypedIndexToString[Inputs](new IndexToString().setLabels(labels), inputsChecker.inputCol)
  }
}