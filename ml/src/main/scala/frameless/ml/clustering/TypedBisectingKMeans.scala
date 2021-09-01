package frameless
package ml
package classification

import frameless.ml.internals.VectorInputsChecker
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}

/**
 * A bisecting k-means algorithm based on the paper "A comparison of document clustering techniques"
 * by Steinbach, Karypis, and Kumar, with modification to fit Spark.
 * The algorithm starts from a single cluster that contains all points.
 * Iteratively it finds divisible clusters on the bottom level and bisects each of them using
 * k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
 * The bisecting steps of clusters on the same level are grouped together to increase parallelism.
 * If bisecting all divisible clusters on the bottom level would result more than `k` leaf clusters,
 * larger clusters get higher priority.
 *
 * @see <a href="http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf">
 * Steinbach, Karypis, and Kumar, A comparison of document clustering techniques,
 * KDD Workshop on Text Mining, 2000.</a>
 */
class TypedBisectingKMeans[Inputs] private[ml] (
    bkm: BisectingKMeans,
    featuresCol: String
) extends TypedEstimator[Inputs, TypedBisectingKMeans.Output, BisectingKMeansModel] {
  val estimator: BisectingKMeans =
    bkm.setFeaturesCol(featuresCol).setPredictionCol(AppendTransformer.tempColumnName)

  def setK(value: Int): TypedBisectingKMeans[Inputs] = copy(bkm.setK(value))

  def setMaxIter(value: Int): TypedBisectingKMeans[Inputs] = copy(bkm.setMaxIter(value))

  def setMinDivisibleClusterSize(value: Double): TypedBisectingKMeans[Inputs] =
    copy(bkm.setMinDivisibleClusterSize(value))

  def setSeed(value: Long): TypedBisectingKMeans[Inputs] = copy(bkm.setSeed(value))

  private def copy(newBkm: BisectingKMeans): TypedBisectingKMeans[Inputs] =
    new TypedBisectingKMeans[Inputs](newBkm, featuresCol)
}

object TypedBisectingKMeans {
  case class Output(prediction: Int)

  def apply[Inputs]()(
      implicit inputsChecker: VectorInputsChecker[Inputs]): TypedBisectingKMeans[Inputs] =
    new TypedBisectingKMeans(new BisectingKMeans(), inputsChecker.featuresCol)
}
