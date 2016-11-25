package frameless
package functions

/**
  * When taking variance, Spark doesn't change this type:
  * - Double     -> Double
  */
trait Variance[T]

object Variance {
  implicit val doubleVariance: Variance[Double] = new Variance[Double] {}
}
