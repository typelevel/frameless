package frameless

import scala.annotation.implicitNotFound

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
@implicitNotFound("Cannot compute sum of type ${In}.")
trait CatalystSummable[In, Out] {
  def zero: In
}

object CatalystSummable {
  def apply[In, Out](zero: In): CatalystSummable[In, Out] = {
    val _zero = zero
    new CatalystSummable[In, Out] { val zero: In = _zero }
  }

  implicit val framelessSummableLong      : CatalystSummable[Long, Long]             = CatalystSummable(zero = 0L)
  implicit val framelessSummableBigDecimal: CatalystSummable[BigDecimal, BigDecimal] = CatalystSummable(zero = BigDecimal(0))
  implicit val framelessSummableDouble    : CatalystSummable[Double, Double]         = CatalystSummable(zero = 0.0)
  implicit val framelessSummableInt       : CatalystSummable[Int, Long]              = CatalystSummable(zero = 0)
  implicit val framelessSummableShort     : CatalystSummable[Short, Long]            = CatalystSummable(zero = 0)
}
