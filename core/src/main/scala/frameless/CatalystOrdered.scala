package frameless

/** Types that can be ordered/compared by Catalyst. */
trait CatalystOrdered[A]

object CatalystOrdered {
  implicit object IntOrdered extends CatalystOrdered[Int]
  implicit object BooleanOrdered extends CatalystOrdered[Boolean]
  implicit object ByteOrdered extends CatalystOrdered[Byte]
  implicit object ShortOrdered extends CatalystOrdered[Short]
  implicit object LongOrdered extends CatalystOrdered[Long]
  implicit object FloatOrdered extends CatalystOrdered[Float]
  implicit object DoubleOrdered extends CatalystOrdered[Double]
  implicit object BigDecimalOrdered extends CatalystOrdered[BigDecimal]
  implicit object SQLDateOrdered extends CatalystOrdered[SQLDate]
  implicit object SQLTimestampOrdered extends CatalystOrdered[SQLTimestamp]
  implicit object StringOrdered extends CatalystOrdered[String]
}
