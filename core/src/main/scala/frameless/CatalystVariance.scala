package frameless

/**
  * When taking the variance or stddev Spark doesn't change these types:
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  * But it changes these types :
  * - Int        -> Double
  * - Short      -> Double
  * - Long       -> Double
  */
trait CatalystVariance[In, Out]

object CatalystVariance {
  implicit val varianceBigDecimal: CatalystVariance[BigDecimal, BigDecimal] = new CatalystVariance[BigDecimal, BigDecimal] {}
  implicit val varianceDouble: CatalystVariance[Double, Double] = new CatalystVariance[Double, Double] {}
  implicit val varianceLong: CatalystVariance[Long, Double] = new CatalystVariance[Long, Double] {}
  implicit val varianceInt: CatalystVariance[Int, Double] = new CatalystVariance[Int, Double] {}
  implicit val varianceShort: CatalystVariance[Short, Double] = new CatalystVariance[Short, Double] {}
}