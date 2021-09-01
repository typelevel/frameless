package frameless
package ml
package feature

import org.apache.spark.ml.feature.IndexToString

import frameless.ml.internals.UnaryInputsChecker

/**
 * A `TypedTransformer` that maps a column of indices back to a new column of corresponding
 * string values. The index-string mapping must be supplied when creating the
 * `TypedIndexToString`.
 *
 * @see
 *   `TypedStringIndexer` for converting strings into indices
 */
final class TypedIndexToString[Inputs] private[ml] (
    indexToString: IndexToString,
    inputCol: String)
    extends AppendTransformer[Inputs, TypedIndexToString.Outputs, IndexToString] {

  val transformer: IndexToString =
    indexToString.setInputCol(inputCol).setOutputCol(AppendTransformer.tempColumnName)

}

object TypedIndexToString {
  case class Outputs(originalOutput: String)

  def apply[Inputs](labels: Array[String])(
      implicit
      inputsChecker: UnaryInputsChecker[Inputs, Double]): TypedIndexToString[Inputs] = {
    new TypedIndexToString[Inputs](
      new IndexToString().setLabels(labels),
      inputsChecker.inputCol)
  }
}
