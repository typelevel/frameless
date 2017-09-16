package frameless

import scala.annotation.implicitNotFound

/** Types that can be added, subtracted and multiplied by Catalyst. */
@implicitNotFound("Cannot do numeric operations on columns of type ${A}.")
trait CatalystNumeric[A]

object CatalystNumeric {
  implicit object bigDecimalNumeric extends CatalystNumeric[BigDecimal]
  implicit object byteNumeric extends CatalystNumeric[Byte]
  implicit object doubleNumeric extends CatalystNumeric[Double]
  implicit object intNumeric extends CatalystNumeric[Int]
  implicit object longNumeric extends CatalystNumeric[Long]
  implicit object shortNumeric extends CatalystNumeric[Short]
}
