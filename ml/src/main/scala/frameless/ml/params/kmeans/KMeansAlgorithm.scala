package frameless.ml.params.kmeans

/**
  * Param for the initialization algorithm.
  * This can be either "random" to choose random points as
  * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
  * (Bahmani et al., Scalable K-Means++, VLDB 2012).
  * Default: k-means||.
  */

sealed abstract class KMeansAlgorithm private[ml](val sparkValue: String)

object KMeansAlgorithm{
  case object Random extends KMeansAlgorithm("random")
  case object KMeansPlusPlus extends KMeansAlgorithm("k-means||")
}
