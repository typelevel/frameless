package frameless
package ml
package feature

import frameless.ml.feature.TypedStringIndexer.HandleInvalid
import frameless.ml.internals.UnaryInputsChecker
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}

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
                    (implicit inputsChecker: UnaryInputsChecker[Inputs, String]): TypedStringIndexer[Inputs] = {
    new TypedStringIndexer[Inputs](new StringIndexer(), inputsChecker.inputCol)
  }
}