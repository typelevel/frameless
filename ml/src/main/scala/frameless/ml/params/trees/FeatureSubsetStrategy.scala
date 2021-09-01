package frameless
package ml
package params
package trees

/**
 * The number of features to consider for splits at each tree node.
 * Supported options:
 *  - Auto: Choose automatically for task:
 *            If numTrees == 1, set to All
 *            If numTrees > 1 (forest), set to Sqrt for classification and
 *              to OneThird for regression.
 *  - All: use all features
 *  - OneThird: use 1/3 of the features
 *  - Sqrt: use sqrt(number of features)
 *  - Log2: use log2(number of features)
 *  - Ratio: use (ratio * number of features) features
 *  - NumberOfFeatures: use numberOfFeatures features.
 * (default = Auto)
 *
 * These various settings are based on the following references:
 *  - log2: tested in Breiman (2001)
 *  - sqrt: recommended by Breiman manual for random forests
 *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
 *    package.
 *
 * @see <a href="http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf">Breiman (2001)</a>
 * @see <a href="http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf">
 * Breiman manual for random forests</a>
 */
sealed abstract class FeatureSubsetStrategy private[ml] (val sparkValue: String)
object FeatureSubsetStrategy {
  case object Auto extends FeatureSubsetStrategy("auto")
  case object All extends FeatureSubsetStrategy("all")
  case object OneThird extends FeatureSubsetStrategy("onethird")
  case object Sqrt extends FeatureSubsetStrategy("sqrt")
  case object Log2 extends FeatureSubsetStrategy("log2")
  case class Ratio(value: Double) extends FeatureSubsetStrategy(value.toString)
  case class NumberOfFeatures(value: Int) extends FeatureSubsetStrategy(value.toString)
}
