package frameless
package functions

/**
  * When summing Spark doesn't change these types:
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  */
trait Averagable[T]

object Averagable {
  implicit val bigDecimalAveragable: Averagable[BigDecimal] = new Averagable[BigDecimal] {}
  implicit val doubleAveragable: Averagable[Double] = new Averagable[Double] {}
}
