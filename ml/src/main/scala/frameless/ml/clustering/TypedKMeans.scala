package frameless
package ml
package classification

import frameless.ml.internals.VectorInputsChecker
import frameless.ml.params.kmeans.KMeansAlgorithm
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

/**
  * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
  *
  * @see <a href="http://dx.doi.org/10.14778/2180912.2180915">Bahmani et al., Scalable k-means++.</a>
  */
class TypedKMeans[Inputs] private[ml] (
  km: KMeans,
  featuresCol: String,
  k: Int
) extends TypedEstimator[Inputs,TypedKMeans.Output,KMeansModel] {
  val estimator: KMeans =
    km
      .setK(k)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(AppendTransformer.tempColumnName)

  def setK(value: Int): TypedKMeans[Inputs] = copy(km.setK(value))

  def setInitMode(value: KMeansAlgorithm): TypedKMeans[Inputs] = copy(km.setInitMode(value.sparkValue))

  def setInitSteps(value: Int): TypedKMeans[Inputs] = copy(km.setInitSteps(value))

  def setMaxIter(value: Int): TypedKMeans[Inputs] = copy(km.setMaxIter(value))

  def setTol(value: Double): TypedKMeans[Inputs] = copy(km.setTol(value))

  def setSeed(value: Long): TypedKMeans[Inputs] = copy(km.setSeed(value))

  private def copy(newRf: KMeans): TypedKMeans[Inputs] = new TypedKMeans[Inputs](newRf, featuresCol, k)

}

object TypedKMeans{
  case class Output(prediction: Int)

  def apply[Inputs](k: Int)(implicit inputsChecker: VectorInputsChecker[Inputs]): TypedKMeans[Inputs] = {
    new TypedKMeans(new KMeans(), inputsChecker.featuresCol,k)
  }
}
