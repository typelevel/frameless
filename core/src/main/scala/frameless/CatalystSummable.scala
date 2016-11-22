package frameless

/**
  * When summing Spark doesn't change these types:
  * - Long       -> Long
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  *
  * For other types there are conversions:
  * - Int        -> Long
  * - Short      -> Long
  */
trait CatalystSummable[A, Out] {
  def zero: Out
}

object CatalystSummable {
  def apply[A, Out](zero: Out): CatalystSummable[A, Out] = {
    val _zero = zero
    new CatalystSummable[A, Out] { val zero: Out = _zero }
  }

  implicit val summableLong: CatalystSummable[Long, Long] = CatalystSummable(zero = 0L)
  implicit val summableBigDecimal: CatalystSummable[BigDecimal, BigDecimal] = CatalystSummable(zero = BigDecimal(0))
  implicit val summableDouble: CatalystSummable[Double, Double] = CatalystSummable(zero = 0.0)
  implicit val summableInt: CatalystSummable[Int, Long] = CatalystSummable(zero = 0L)
  implicit val summableShort: CatalystSummable[Short, Long] = CatalystSummable(zero = 0L)
}
