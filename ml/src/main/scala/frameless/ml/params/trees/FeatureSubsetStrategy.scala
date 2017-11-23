package frameless
package ml
package params
package trees

sealed abstract class FeatureSubsetStrategy private[ml](val sparkValue: String)
object FeatureSubsetStrategy {
  case object Auto extends FeatureSubsetStrategy("auto")
  case object All extends FeatureSubsetStrategy("all")
  case object OneThird extends FeatureSubsetStrategy("onethird")
  case object Sqrt extends FeatureSubsetStrategy("sqrt")
  case object Log2 extends FeatureSubsetStrategy("log2")
  case class Ratio(value: Double) extends FeatureSubsetStrategy(value.toString)
  case class NumberOfFeatures(value: Int) extends FeatureSubsetStrategy(value.toString)
}