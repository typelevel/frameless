package frameless
package ml
package classification

import frameless.ml.internals.TreesInputsChecker
import frameless.ml.params.trees.FeatureSubsetStrategy
import org.apache.spark.ml.classification.{
  RandomForestClassificationModel,
  RandomForestClassifier
}
import org.apache.spark.ml.linalg.Vector

/**
 * <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> learning algorithm for
 * classification.
 * It supports both binary and multiclass labels, as well as both continuous and categorical
 * features.
 */
final class TypedRandomForestClassifier[Inputs] private[ml] (
    rf: RandomForestClassifier,
    labelCol: String,
    featuresCol: String
) extends TypedEstimator[
      Inputs,
      TypedRandomForestClassifier.Outputs,
      RandomForestClassificationModel] {

  val estimator: RandomForestClassifier =
    rf.setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(AppendTransformer.tempColumnName)
      .setRawPredictionCol(AppendTransformer.tempColumnName2)
      .setProbabilityCol(AppendTransformer.tempColumnName3)

  def setNumTrees(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setNumTrees(value))
  def setMaxDepth(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setMaxDepth(value))
  def setMinInfoGain(value: Double): TypedRandomForestClassifier[Inputs] = copy(
    rf.setMinInfoGain(value))
  def setMinInstancesPerNode(value: Int): TypedRandomForestClassifier[Inputs] = copy(
    rf.setMinInstancesPerNode(value))
  def setMaxMemoryInMB(value: Int): TypedRandomForestClassifier[Inputs] = copy(
    rf.setMaxMemoryInMB(value))
  def setSubsamplingRate(value: Double): TypedRandomForestClassifier[Inputs] = copy(
    rf.setSubsamplingRate(value))
  def setFeatureSubsetStrategy(
      value: FeatureSubsetStrategy): TypedRandomForestClassifier[Inputs] =
    copy(rf.setFeatureSubsetStrategy(value.sparkValue))
  def setMaxBins(value: Int): TypedRandomForestClassifier[Inputs] = copy(rf.setMaxBins(value))

  private def copy(newRf: RandomForestClassifier): TypedRandomForestClassifier[Inputs] =
    new TypedRandomForestClassifier[Inputs](newRf, labelCol, featuresCol)
}

object TypedRandomForestClassifier {
  case class Outputs(rawPrediction: Vector, probability: Vector, prediction: Double)

  def apply[Inputs](
      implicit
      inputsChecker: TreesInputsChecker[Inputs]): TypedRandomForestClassifier[Inputs] = {
    new TypedRandomForestClassifier(
      new RandomForestClassifier(),
      inputsChecker.labelCol,
      inputsChecker.featuresCol)
  }
}
