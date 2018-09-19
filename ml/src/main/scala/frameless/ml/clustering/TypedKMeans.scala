package frameless
package ml
package classification

import frameless.ml.internals.VectorInputsChecker
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

class TypedKMeans[Inputs] private[ml] (
  km: KMeans,
  featuresCol: String,
  k: Int
) extends TypedEstimator[Inputs,TypedKMeans.Output,KMeansModel] {
  val estimator: KMeans=
    km
      .setK(k)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(AppendTransformer.tempColumnName)

  def setFeaturesCol(value: String): TypedKMeans[Inputs] = copy(km.setFeaturesCol(value))

  def setPredictionCol(value: String): TypedKMeans[Inputs] = copy(km.setPredictionCol(value))

  def setK(value: Int): TypedKMeans[Inputs] = copy(km.setK(value))

  def setInitMode(value: String): TypedKMeans[Inputs] = copy(km.setInitMode(value))

  def setInitSteps(value: Int): TypedKMeans[Inputs] = copy(km.setInitSteps(value))

  def setMaxIter(value: Int): TypedKMeans[Inputs] = copy(km.setMaxIter(value))

  def setTol(value: Double): TypedKMeans[Inputs] = copy(km.setTol(value))

  def setSeed(value: Long): TypedKMeans[Inputs] = copy(km.setSeed(value))

  private def copy(newRf: KMeans): TypedKMeans[Inputs] = new TypedKMeans[Inputs](newRf, featuresCol, k)

}

object TypedKMeans{
  case class Output(prediction: Int)

  def apply[Inputs](k: Int = 20)(implicit inputsChecker: VectorInputsChecker[Inputs]): TypedKMeans[Inputs] = {
    new TypedKMeans(new KMeans(), inputsChecker.featuresCol,k)
  }
}
