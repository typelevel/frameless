package frameless
package functions

/**
  * When summing Spark doesn't change these types:
  * - Long       -> Long
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  *
  * For other types there are conversions:
  * - Int        -> Long
  * - ...
  */
trait Summable[T]

object Summable {
  implicit val summableLong: Summable[Long] = new Summable[Long] {}
  implicit val summableBigDecimal: Summable[BigDecimal] = new Summable[BigDecimal] {}
  implicit val summableDouble: Summable[Double] = new Summable[Double] {}
}
