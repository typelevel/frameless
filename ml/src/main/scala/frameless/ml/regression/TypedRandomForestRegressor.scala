package frameless
package ml
package regression

import frameless.ml.internals.TreesInputsChecker
import frameless.ml.regression.TypedRandomForestRegressor.FeatureSubsetStrategy
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

final class TypedRandomForestRegressor[Inputs] private[ml](
  rf: RandomForestRegressor,
  labelCol: String,
  featuresCol: String
) extends TypedEstimator[Inputs, TypedRandomForestRegressor.Outputs, RandomForestRegressionModel] {

  val estimator: RandomForestRegressor =
    rf
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(AppendTransformer.tempColumnName)

  def setNumTrees(value: Int): TypedRandomForestRegressor[Inputs] = copy(rf.setNumTrees(value))
  def setMaxDepth(value: Int): TypedRandomForestRegressor[Inputs] = copy(rf.setMaxDepth(value))
  def setMinInfoGain(value: Double): TypedRandomForestRegressor[Inputs] = copy(rf.setMinInfoGain(value))
  def setMinInstancesPerNode(value: Int): TypedRandomForestRegressor[Inputs] = copy(rf.setMinInstancesPerNode(value))
  def setMaxMemoryInMB(value: Int): TypedRandomForestRegressor[Inputs] = copy(rf.setMaxMemoryInMB(value))
  def setSubsamplingRate(value: Double): TypedRandomForestRegressor[Inputs] = copy(rf.setSubsamplingRate(value))
  def setFeatureSubsetStrategy(value: FeatureSubsetStrategy): TypedRandomForestRegressor[Inputs] =
    copy(rf.setFeatureSubsetStrategy(value.sparkValue))
  def setMaxBins(value: Int): TypedRandomForestRegressor[Inputs] = copy(rf.setMaxBins(value))

  private def copy(newRf: RandomForestRegressor): TypedRandomForestRegressor[Inputs] =
    new TypedRandomForestRegressor[Inputs](newRf, labelCol, featuresCol)
}

object TypedRandomForestRegressor {
  case class Outputs(prediction: Double)

  sealed trait FeatureSubsetStrategy {
    val sparkValue: String
  }
  object FeatureSubsetStrategy {
    case object Auto extends FeatureSubsetStrategy {
      val sparkValue = "auto"
    }
    case object All extends FeatureSubsetStrategy {
      val sparkValue = "all"
    }
    case object OneThird extends FeatureSubsetStrategy {
      val sparkValue = "onethird"
    }
    case object Sqrt extends FeatureSubsetStrategy {
      val sparkValue = "sqrt"
    }
    case object Log2 extends FeatureSubsetStrategy {
      val sparkValue = "log2"
    }
    case class StrictlyPositiveDouble(value: Double) extends FeatureSubsetStrategy {
      val sparkValue = value.toString
    }
  }

  def create[Inputs]()(implicit inputsChecker: TreesInputsChecker[Inputs])
  : TypedRandomForestRegressor[Inputs] = {
    new TypedRandomForestRegressor(new RandomForestRegressor(), inputsChecker.labelCol, inputsChecker.featuresCol)
  }
}