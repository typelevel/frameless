package frameless

trait CatalystCast[A, B]

object CatalystCast {
  private[this] val theInstance = new CatalystCast[Any, Any] {}
  private[this] def of[A, B]: CatalystCast[A, B] = theInstance.asInstanceOf[CatalystCast[A, B]]

  implicit def framelessCastToString[T]: CatalystCast[T, String] = of[T, String]

  implicit def framelessNumericToLong   [A: CatalystNumeric]: CatalystCast[A, Long]       = of[A, Long]
  implicit def framelessNumericToInt    [A: CatalystNumeric]: CatalystCast[A, Int]        = of[A, Int]
  implicit def framelessNumericToShort  [A: CatalystNumeric]: CatalystCast[A, Short]      = of[A, Short]
  implicit def framelessNumericToByte   [A: CatalystNumeric]: CatalystCast[A, Byte]       = of[A, Byte]
  implicit def framelessNumericToDecimal[A: CatalystNumeric]: CatalystCast[A, BigDecimal] = of[A, BigDecimal]
  implicit def framelessNumericToDouble [A: CatalystNumeric]: CatalystCast[A, Double]     = of[A, Double]

  implicit def framelessBooleanToNumeric[A: CatalystNumeric]: CatalystCast[Boolean, A] = of[Boolean, A]

  // doesn't make any sense to include:
  // - sqlDateToBoolean: always None
  // - sqlTimestampToBoolean: compares us to 0
  implicit val framelessStringToBoolean    : CatalystCast[String, Option[Boolean]] = of[String, Option[Boolean]]
  implicit val framelessLongToBoolean      : CatalystCast[Long, Boolean]           = of[Long, Boolean]
  implicit val framelessIntToBoolean       : CatalystCast[Int, Boolean]            = of[Int, Boolean]
  implicit val framelessShortToBoolean     : CatalystCast[Short, Boolean]          = of[Short, Boolean]
  implicit val framelessByteToBoolean      : CatalystCast[Byte, Boolean]           = of[Byte, Boolean]
  implicit val framelessBigDecimalToBoolean: CatalystCast[BigDecimal, Boolean]     = of[BigDecimal, Boolean]
  implicit val framelessDoubleToBoolean    : CatalystCast[Double, Boolean]         = of[Double, Boolean]

  // TODO

  // needs verification, does it make sense to include? probably better as a separate function
  // implicit object stringToInt extends CatalystCast[String, Option[Int]]
  // implicit object stringToShort extends CatalystCast[String, Option[Short]]
  // implicit object stringToByte extends CatalystCast[String, Option[Byte]]
  // implicit object stringToDecimal extends CatalystCast[String, Option[BigDecimal]]
  // implicit object stringToLong extends CatalystCast[String, Option[Long]]
  // implicit object stringToSqlDate extends CatalystCast[String, Option[SQLDate]]


  // needs verification:
  //implicit object sqlTimestampToSqlDate extends CatalystCast[SQLTimestamp, SQLDate]

  // needs verification:
  // implicit object sqlTimestampToDecimal extends CatalystCast[SQLTimestamp, BigDecimal]
  // implicit object sqlTimestampToLong extends CatalystCast[SQLTimestamp, Long]

  // needs verification:
  // implicit object stringToSqlTimestamp extends CatalystCast[String, SQLTimestamp]
  // implicit object longToSqlTimestamp extends CatalystCast[Long, SQLTimestamp]
  // implicit object intToSqlTimestamp extends CatalystCast[Int, SQLTimestamp]
  // implicit object doubleToSqlTimestamp extends CatalystCast[Double, SQLTimestamp]
  // implicit object floatToSqlTimestamp extends CatalystCast[Float, SQLTimestamp]
  // implicit object bigDecimalToSqlTimestamp extends CatalystCast[BigDecimal, SQLTimestamp]
  // implicit object sqlDateToSqlTimestamp extends CatalystCast[SQLDate, SQLTimestamp]

  // doesn't make sense to include:
  // - booleanToSqlTimestamp: 1L or 0L
  // - shortToSqlTimestamp: ???
  // - byteToSqlTimestamp: ???

  // doesn't make sense to include:
  // - sqlDateToLong: always None
  // - sqlDateToInt: always None
  // - sqlDateToInt: always None
  // - sqlDateToInt: always None
  // - sqlDateToInt: always None

  // doesn't make sense to include:
  // - sqlTimestampToInt: useful? can be done through `-> Long -> Int`
  // - sqlTimestampToShort: useful? can be done through `-> Long -> Int`
  // - sqlTimestampToShort: useful? can be done through `-> Long -> Int`

}
