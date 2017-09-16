package frameless

import scala.annotation.implicitNotFound

/** Spark divides everything as Double, expect BigDecimals are divided into
  * another BigDecimal, benefiting from some added precision.
  */
@implicitNotFound("Cannot compute division on type ${In}.")
trait CatalystDivisible[In, Out]

object CatalystDivisible {
  implicit object divisibleBigDecimal extends CatalystDivisible[BigDecimal, BigDecimal]
  implicit object divisibleDouble     extends CatalystDivisible[Double, Double]
  implicit object divisibleInt        extends CatalystDivisible[Int, Double]
  implicit object divisibleLong       extends CatalystDivisible[Long, Double]
  implicit object divisibleByte       extends CatalystDivisible[Byte, Double]
  implicit object divisibleShort      extends CatalystDivisible[Short, Double]
}
