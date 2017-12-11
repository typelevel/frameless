package frameless

import scala.annotation.implicitNotFound

/** Types that can be ordered/compared by Catalyst.
  *
  * @note CatalystOrdered instances are also [[frameless.CatalystRowOrdered]] instances.
  *       If a type is not row orderable by Spark, [[frameless.CatalystRowOrdered.orderedEvidence]]
  *       must be modified or removed
  */
@implicitNotFound("Cannot compare columns of type ${A}.")
trait CatalystOrdered[A]

object CatalystOrdered {
  private[this] val theInstance = new CatalystOrdered[Any] {}
  private[this] def of[A]: CatalystOrdered[A] = theInstance.asInstanceOf[CatalystOrdered[A]]

  implicit val framelessIntOrdered         : CatalystOrdered[Int]          = of[Int]
  implicit val framelessBooleanOrdered     : CatalystOrdered[Boolean]      = of[Boolean]
  implicit val framelessByteOrdered        : CatalystOrdered[Byte]         = of[Byte]
  implicit val framelessShortOrdered       : CatalystOrdered[Short]        = of[Short]
  implicit val framelessLongOrdered        : CatalystOrdered[Long]         = of[Long]
  implicit val framelessFloatOrdered       : CatalystOrdered[Float]        = of[Float]
  implicit val framelessDoubleOrdered      : CatalystOrdered[Double]       = of[Double]
  implicit val framelessBigDecimalOrdered  : CatalystOrdered[BigDecimal]   = of[BigDecimal]
  implicit val framelessSQLDateOrdered     : CatalystOrdered[SQLDate]      = of[SQLDate]
  implicit val framelessSQLTimestampOrdered: CatalystOrdered[SQLTimestamp] = of[SQLTimestamp]
  implicit val framelessStringOrdered      : CatalystOrdered[String]       = of[String]
}
