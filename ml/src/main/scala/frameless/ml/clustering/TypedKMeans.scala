package frameless
package ml
package classification

import frameless.ml.internals.VectorInputsChecker
import frameless.ml.params.kmeans.KMeansInitMode
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

/**
  * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
  *
  * @see <a href="http://dx.doi.org/10.14778/2180912.2180915">Bahmani et al., Scalable k-means++.</a>
  */
class TypedKMeans[Inputs] private[ml] (
  km: KMeans,
  featuresCol: String
) extends TypedEstimator[Inputs,TypedKMeans.Output,KMeansModel] {
  val estimator: KMeans =
    km
      .setFeaturesCol(featuresCol)
      .setPredictionCol(AppendTransformer.tempColumnName)

  def setK(value: Int): TypedKMeans[Inputs] = copy(km.setK(value))

  def setInitMode(value: KMeansInitMode): TypedKMeans[Inputs] = copy(km.setInitMode(value.sparkValue))

  def setInitSteps(value: Int): TypedKMeans[Inputs] = copy(km.setInitSteps(value))

  def setMaxIter(value: Int): TypedKMeans[Inputs] = copy(km.setMaxIter(value))

  def setTol(value: Double): TypedKMeans[Inputs] = copy(km.setTol(value))

  def setSeed(value: Long): TypedKMeans[Inputs] = copy(km.setSeed(value))

  private def copy(newKmeans: KMeans): TypedKMeans[Inputs] = new TypedKMeans[Inputs](newKmeans, featuresCol)

}

object TypedKMeans{
  case class Output(prediction: Int)

  def apply[Inputs](implicit inputsChecker: VectorInputsChecker[Inputs]): TypedKMeans[Inputs] = {
    new TypedKMeans(new KMeans(), inputsChecker.featuresCol)
  }
}
