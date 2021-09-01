package frameless
package ml
package regression

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

import frameless.ml.{AppendTransformer, TypedEstimator}
import frameless.ml.internals.LinearInputsChecker
import frameless.ml.params.linears.{LossStrategy, Solver}

/**
 * <a href="https://en.wikipedia.org/wiki/Linear_regression">Linear Regression</a> linear
 * approach to modelling the relationship between a scalar response (or dependent variable) and
 * one or more explanatory variables
 */
final class TypedLinearRegression[Inputs] private[ml] (
    lr: LinearRegression,
    labelCol: String,
    featuresCol: String,
    weightCol: Option[String]
) extends TypedEstimator[Inputs, TypedLinearRegression.Outputs, LinearRegressionModel] {

  val estimatorWithoutWeight: LinearRegression = lr
    .setLabelCol(labelCol)
    .setFeaturesCol(featuresCol)
    .setPredictionCol(AppendTransformer.tempColumnName)

  val estimator =
    if (weightCol.isDefined) estimatorWithoutWeight.setWeightCol(weightCol.get)
    else estimatorWithoutWeight

  def setRegParam(value: Double): TypedLinearRegression[Inputs] = copy(lr.setRegParam(value))
  def setFitIntercept(value: Boolean): TypedLinearRegression[Inputs] = copy(
    lr.setFitIntercept(value))
  def setStandardization(value: Boolean): TypedLinearRegression[Inputs] = copy(
    lr.setStandardization(value))
  def setElasticNetParam(value: Double): TypedLinearRegression[Inputs] = copy(
    lr.setElasticNetParam(value))
  def setMaxIter(value: Int): TypedLinearRegression[Inputs] = copy(lr.setMaxIter(value))
  def setTol(value: Double): TypedLinearRegression[Inputs] = copy(lr.setTol(value))
  def setSolver(value: Solver): TypedLinearRegression[Inputs] = copy(
    lr.setSolver(value.sparkValue))
  def setAggregationDepth(value: Int): TypedLinearRegression[Inputs] = copy(
    lr.setAggregationDepth(value))
  def setLoss(value: LossStrategy): TypedLinearRegression[Inputs] = copy(
    lr.setLoss(value.sparkValue))
  def setEpsilon(value: Double): TypedLinearRegression[Inputs] = copy(lr.setEpsilon(value))

  private def copy(newLr: LinearRegression): TypedLinearRegression[Inputs] =
    new TypedLinearRegression[Inputs](newLr, labelCol, featuresCol, weightCol)

}

object TypedLinearRegression {
  case class Outputs(prediction: Double)
  case class Weight(weight: Double)

  def apply[Inputs](
      implicit inputsChecker: LinearInputsChecker[Inputs]): TypedLinearRegression[Inputs] = {
    new TypedLinearRegression(
      new LinearRegression(),
      inputsChecker.labelCol,
      inputsChecker.featuresCol,
      inputsChecker.weightCol)
  }
}
