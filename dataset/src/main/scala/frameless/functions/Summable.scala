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
trait Summable[A] {
  def zero: A
}

object Summable {
  def apply[A](zero: A): Summable[A] = {
    val _zero = zero
    new Summable[A] { val zero: A = _zero }
  }

  implicit val summableLong: Summable[Long] = Summable(zero = 0L)
  implicit val summableBigDecimal: Summable[BigDecimal] = Summable(zero = BigDecimal(0))
  implicit val summableDouble: Summable[Double] = Summable(zero = 0.0)
}
