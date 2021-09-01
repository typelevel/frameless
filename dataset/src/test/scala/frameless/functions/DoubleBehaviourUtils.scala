package frameless
package functions

/**
 * Some statistical functions in Spark can result in Double, Double.NaN or Null. This tends to
 * break ?= of the property based testing. Use the nanNullHandler function here to alleviate
 * this by mapping this NaN and Null to None. This will result in functioning comparison again.
 */
object DoubleBehaviourUtils {
  // Mapping with this function is needed because spark uses Double.NaN for some semantics in the
  // correlation function. ?= for prop testing will use == underlying and will break because Double.NaN != Double.NaN
  private val nanHandler: Double => Option[Double] = value =>
    if (!value.equals(Double.NaN)) Option(value) else None
  // Making sure that null => None and does not result in 0.0d because of row.getAs[Double]'s use of .asInstanceOf
  val nanNullHandler: Any => Option[Double] = {
    case null => None
    case d: Double => nanHandler(d)
    case _ => ???
  }
}
