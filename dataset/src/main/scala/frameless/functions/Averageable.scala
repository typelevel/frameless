package frameless
package functions

/**
  * When summing Spark doesn't change these types:
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  */
trait Averageable[T]

object Averageable {
  implicit val bigDecimalAveragable: Averageable[BigDecimal] = new Averageable[BigDecimal] {}
  implicit val doubleAveragable: Averageable[Double] = new Averageable[Double] {}
}
