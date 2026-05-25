package frameless

import scala.annotation.implicitNotFound

/**
 * When averaging Spark doesn't change these types:
 * - BigDecimal -> BigDecimal
 * - Double     -> Double
 * But it changes these types :
 * - Int        -> Double
 * - Short      -> Double
 * - Long       -> Double
 */
@implicitNotFound("Cannot compute average of type ${In}.")
trait CatalystAverageable[In, Out]

object CatalystAverageable {
  private[this] val theInstance = new CatalystAverageable[Any, Any] {}

  private[this] def of[In, Out]: CatalystAverageable[In, Out] =
    theInstance.asInstanceOf[CatalystAverageable[In, Out]]

  implicit val framelessAverageableBigDecimal: CatalystAverageable[BigDecimal, BigDecimal] =
    of[BigDecimal, BigDecimal]

  implicit val framelessAverageableDouble: CatalystAverageable[Double, Double] =
    of[Double, Double]

  implicit val framelessAverageableLong: CatalystAverageable[Long, Double] =
    of[Long, Double]

  implicit val framelessAverageableInt: CatalystAverageable[Int, Double] =
    of[Int, Double]

  implicit val framelessAverageableShort: CatalystAverageable[Short, Double] =
    of[Short, Double]
}
