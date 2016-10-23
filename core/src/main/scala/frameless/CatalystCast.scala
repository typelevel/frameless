package frameless

trait CatalystCast[A, B]

object CatalystCast {
  implicit def castToString[T]: CatalystCast[T, String] = new CatalystCast[T, String] {}

  implicit def numericToLong[A: CatalystNumeric]: CatalystCast[A, Long] = new CatalystCast[A, Long] {}
  implicit def numericToInt[A: CatalystNumeric]: CatalystCast[A, Int] = new CatalystCast[A, Int] {}
  implicit def numericToShort[A: CatalystNumeric]: CatalystCast[A, Short] = new CatalystCast[A, Short] {}
  implicit def numericToByte[A: CatalystNumeric]: CatalystCast[A, Byte] = new CatalystCast[A, Byte] {}
  implicit def numericToDecimal[A: CatalystNumeric]: CatalystCast[A, BigDecimal] = new CatalystCast[A, BigDecimal] {}
  implicit def numericToDouble[A: CatalystNumeric]: CatalystCast[A, Double] = new CatalystCast[A, Double] {}

  implicit def booleanToNumeric[A: CatalystNumeric]: CatalystCast[Boolean, A] = new CatalystCast[Boolean, A] {}

  // doesn't make any sense to include:
  // - sqlDateToBoolean: always None
  // - sqlTimestampToBoolean: compares us to 0
  implicit object stringToBoolean extends CatalystCast[String, Option[Boolean]]
  implicit object longToBoolean extends CatalystCast[Long, Boolean]
  implicit object intToBoolean extends CatalystCast[Int, Boolean]
  implicit object shortToBoolean extends CatalystCast[Short, Boolean]
  implicit object byteToBoolean extends CatalystCast[Byte, Boolean]
  implicit object bigDecimalToBoolean extends CatalystCast[BigDecimal, Boolean]
  implicit object doubleToBoolean extends CatalystCast[Double, Boolean]

  // TODO

  // needs verification, does it make sense to include? probably better as a separate function
  // implicit object stringToInt extends CatalystCast[String, Option[Int]]
  // implicit object stringToShort extends CatalystCast[String, Option[Short]]
  // implicit object stringToByte extends CatalystCast[String, Option[Byte]]
  // implicit object stringToDecimal extends CatalystCast[String, Option[BigDecimal]]
  // implicit object stringToLong extends CatalystCast[String, Option[Long]]
  // implicit object stringToSqlDate extends CatalystCast[String, Option[SQLDate]]

  // needs verification:
  // implicit object sqlTimestampToSqlDate extends CatalystCast[SQLTimestamp, SQLDate]

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
