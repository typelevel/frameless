package frameless

import scala.annotation.implicitNotFound

/**
 * Spark divides everything as Double, expect BigDecimals are divided into another BigDecimal,
 * benefiting from some added precision.
 */
@implicitNotFound("Cannot compute division on type ${In}.")
trait CatalystDivisible[In, Out]

object CatalystDivisible {
  private[this] val theInstance = new CatalystDivisible[Any, Any] {}
  private[this] def of[In, Out]: CatalystDivisible[In, Out] =
    theInstance.asInstanceOf[CatalystDivisible[In, Out]]

  implicit val framelessDivisibleBigDecimal: CatalystDivisible[BigDecimal, BigDecimal] =
    of[BigDecimal, BigDecimal]
  implicit val framelessDivisibleDouble: CatalystDivisible[Double, Double] = of[Double, Double]
  implicit val framelessDivisibleInt: CatalystDivisible[Int, Double] = of[Int, Double]
  implicit val framelessDivisibleLong: CatalystDivisible[Long, Double] = of[Long, Double]
  implicit val framelessDivisibleByte: CatalystDivisible[Byte, Double] = of[Byte, Double]
  implicit val framelessDivisibleShort: CatalystDivisible[Short, Double] = of[Short, Double]
}
