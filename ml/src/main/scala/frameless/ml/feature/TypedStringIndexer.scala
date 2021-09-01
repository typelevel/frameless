package frameless
package ml
package feature

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}

import frameless.ml.feature.TypedStringIndexer.HandleInvalid
import frameless.ml.internals.UnaryInputsChecker

/**
 * A label indexer that maps a string column of labels to an ML column of label indices. The
 * indices are in [0, numLabels), ordered by label frequencies. So the most frequent label gets
 * index 0.
 *
 * @see
 *   `TypedIndexToString` for the inverse transformation
 */
final class TypedStringIndexer[Inputs] private[ml] (
    stringIndexer: StringIndexer,
    inputCol: String)
    extends TypedEstimator[Inputs, TypedStringIndexer.Outputs, StringIndexerModel] {

  val estimator: StringIndexer =
    stringIndexer.setInputCol(inputCol).setOutputCol(AppendTransformer.tempColumnName)

  def setHandleInvalid(value: HandleInvalid): TypedStringIndexer[Inputs] = copy(
    stringIndexer.setHandleInvalid(value.sparkValue))

  private def copy(newStringIndexer: StringIndexer): TypedStringIndexer[Inputs] =
    new TypedStringIndexer[Inputs](newStringIndexer, inputCol)
}

object TypedStringIndexer {
  case class Outputs(indexedOutput: Double)

  sealed abstract class HandleInvalid(val sparkValue: String)
  object HandleInvalid {
    case object Error extends HandleInvalid("error")
    case object Skip extends HandleInvalid("skip")
    case object Keep extends HandleInvalid("keep")
  }

  def apply[Inputs](
      implicit
      inputsChecker: UnaryInputsChecker[Inputs, String]): TypedStringIndexer[Inputs] = {
    new TypedStringIndexer[Inputs](new StringIndexer(), inputsChecker.inputCol)
  }
}
