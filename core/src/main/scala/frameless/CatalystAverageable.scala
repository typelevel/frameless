package frameless

/**
  * When summing Spark doesn't change these types:
  * - BigDecimal -> BigDecimal
  * - Double     -> Double
  */
trait CatalystAverageable[In, Out]

object CatalystAverageable {
  implicit val bigDecimalAveragable: CatalystAverageable[BigDecimal, BigDecimal] = new CatalystAverageable[BigDecimal, BigDecimal] {}
  implicit val doubleAveragable: CatalystAverageable[Double, Double] = new CatalystAverageable[Double, Double] {}
  implicit val LongAveragable: CatalystAverageable[Long, Double] = new CatalystAverageable[Long, Double] {}
  implicit val IntAveragable: CatalystAverageable[Int, Double] = new CatalystAverageable[Int, Double] {}
  implicit val ShortAveragable: CatalystAverageable[Short, Double] = new CatalystAverageable[Short, Double] {}
}
