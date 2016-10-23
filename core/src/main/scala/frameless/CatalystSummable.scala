package frameless

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
trait CatalystSummable[A] {
  def zero: A
}

object CatalystSummable {
  def apply[A](zero: A): CatalystSummable[A] = {
    val _zero = zero
    new CatalystSummable[A] { val zero: A = _zero }
  }

  implicit val summableLong: CatalystSummable[Long] = CatalystSummable(zero = 0L)
  implicit val summableBigDecimal: CatalystSummable[BigDecimal] = CatalystSummable(zero = BigDecimal(0))
  implicit val summableDouble: CatalystSummable[Double] = CatalystSummable(zero = 0.0)
}
