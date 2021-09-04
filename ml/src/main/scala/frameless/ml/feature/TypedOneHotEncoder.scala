package frameless.ml.feature

import frameless.ml.feature.TypedOneHotEncoder.HandleInvalid
import frameless.ml.internals.UnaryInputsChecker
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, OneHotEncoderModel}
import org.apache.spark.ml.linalg.Vector

/**
 * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
 * at most a single one-value per row that indicates the input category index.
 *
 *  @see `TypedStringIndexer` for converting categorical values into category indices
 */
class TypedOneHotEncoder[Inputs] private[ml](oneHotEncoder: OneHotEncoderEstimator, inputCol: String)
  extends TypedEstimator[Inputs, TypedOneHotEncoder.Outputs, OneHotEncoderModel] {

  override val estimator: Estimator[OneHotEncoderModel] = oneHotEncoder
    .setInputCols(Array(inputCol))
    .setOutputCols(Array(AppendTransformer.tempColumnName))

  def setHandleInvalid(value: HandleInvalid): TypedOneHotEncoder[Inputs] =
    copy(oneHotEncoder.setHandleInvalid(value.sparkValue))

  def setDropLast(value: Boolean): TypedOneHotEncoder[Inputs] =
    copy(oneHotEncoder.setDropLast(value))

  private def copy(newOneHotEncoder: OneHotEncoderEstimator): TypedOneHotEncoder[Inputs] =
    new TypedOneHotEncoder[Inputs](newOneHotEncoder, inputCol)
}

object TypedOneHotEncoder {

  case class Outputs(output: Vector)

  final class HandleInvalid private(val sparkValue: String) extends AnyVal

  object HandleInvalid {
    case object Error extends HandleInvalid("error")
    case object Keep extends HandleInvalid("keep")
  }

  def apply[T](implicit inputsChecker: UnaryInputsChecker[T, Int]): TypedOneHotEncoder[T] =
    new TypedOneHotEncoder[Inputs](new OneHotEncoderEstimator(), inputsChecker.inputCol)
}
