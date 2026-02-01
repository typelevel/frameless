package frameless

import scala.annotation.implicitNotFound

/**
 * Spark does not return always long on round
 */
@implicitNotFound("Cannot compute round on type ${In}.")
trait CatalystRound[In, Out]

object CatalystRound {
  private[this] val theInstance = new CatalystRound[Any, Any] {}

  private[this] def of[In, Out]: CatalystRound[In, Out] =
    theInstance.asInstanceOf[CatalystRound[In, Out]]

  implicit val framelessBigDecimal: CatalystRound[BigDecimal, java.math.BigDecimal] =
    of[BigDecimal, java.math.BigDecimal]
  implicit val framelessDouble: CatalystRound[Double, Long] = of[Double, Long]
  implicit val framelessInt: CatalystRound[Int, Long] = of[Int, Long]
  implicit val framelessLong: CatalystRound[Long, Long] = of[Long, Long]
  implicit val framelessShort: CatalystRound[Short, Long] = of[Short, Long]
}
