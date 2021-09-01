package frameless

import scala.annotation.implicitNotFound

/**
 * Types for which we can check if is in
 */
@implicitNotFound("Cannot do isin operation on columns of type ${A}.")
trait CatalystIsin[A]

object CatalystIsin {
  implicit object framelessBigDecimal extends CatalystIsin[BigDecimal]
  implicit object framelessByte extends CatalystIsin[Byte]
  implicit object framelessDouble extends CatalystIsin[Double]
  implicit object framelessFloat extends CatalystIsin[Float]
  implicit object framelessInt extends CatalystIsin[Int]
  implicit object framelessLong extends CatalystIsin[Long]
  implicit object framelessShort extends CatalystIsin[Short]
  implicit object framelesssString extends CatalystIsin[String]
}
