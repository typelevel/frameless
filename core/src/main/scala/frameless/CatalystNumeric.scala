package frameless

trait CatalystNumeric[A]

object CatalystNumeric {
  implicit object bigDecimalNumeric extends CatalystNumeric[BigDecimal]
  implicit object byteNumeric extends CatalystNumeric[Byte]
  implicit object doubleNumeric extends CatalystNumeric[Double]
  implicit object intNumeric extends CatalystNumeric[Int]
  implicit object longNumeric extends CatalystNumeric[Long]
  implicit object shortNumeric extends CatalystNumeric[Short]
}
