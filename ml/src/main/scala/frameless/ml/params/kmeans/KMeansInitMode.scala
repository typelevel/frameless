package frameless
package ml
package params
package kmeans

/**
 * Param for the initialization algorithm. This can be either "random" to choose random points
 * as initial cluster centers, or "k-means||" to use a parallel variant of k-means++ (Bahmani et
 * al., Scalable K-Means++, VLDB 2012). Default: k-means||.
 */

sealed abstract class KMeansInitMode private[ml] (val sparkValue: String)

object KMeansInitMode {
  case object Random extends KMeansInitMode("random")
  case object KMeansPlusPlus extends KMeansInitMode("k-means||")
}
