package frameless
package functions

import org.scalacheck.Prop
import org.scalacheck.Prop.AnyOperators
import org.scalacheck.util.Pretty
import shapeless.{ Lens, OpticDefns }

/**
 * Some statistical functions in Spark can result in Double, Double.NaN or Null.
 * This tends to break ?= of the property based testing. Use the nanNullHandler function
 * here to alleviate this by mapping this NaN and Null to None. This will result in
 * functioning comparison again.
 *
 *  Values are truncated to allow a chance of mitigating serialization issues
 */
object DoubleBehaviourUtils {

  val dp5 = BigDecimal(0.00001)

  // Mapping with this function is needed because spark uses Double.NaN for some semantics in the
  // correlation function. ?= for prop testing will use == underlying and will break because Double.NaN != Double.NaN
  private val nanHandler: Double => Option[Double] = value =>
    if (!value.equals(Double.NaN)) Option(value) else None

  // Making sure that null => None and does not result in 0.0d because of row.getAs[Double]'s use of .asInstanceOf
  val nanNullHandler: Any => Option[BigDecimal] = {
    case null => None
    case d: Double =>
      nanHandler(d).map(truncate)
    case _ => ???
  }

  /** ensure different serializations are 'comparable' */
  def truncate(d: Double): BigDecimal =
    if (d == Double.NegativeInfinity || d == Double.PositiveInfinity)
      BigDecimal("1000000.000000") * (if (d == Double.PositiveInfinity) 1
                                      else -1)
    else
      BigDecimal(d).setScale(
        6,
        if (d > 0)
          BigDecimal.RoundingMode.FLOOR
        else
          BigDecimal.RoundingMode.CEILING
      )

  import shapeless._

  def tolerantCompareVectors[K, CC[X] <: Seq[X]](
      v1: CC[K],
      v2: CC[K],
      of: BigDecimal
    )(fudgers: Seq[OpticDefns.RootLens[K] => Lens[K, Option[BigDecimal]]]
    ): Prop = compareVectors(v1, v2)(fudgers.map(f => (f, tolerance(_, of))))

  def compareVectors[K, CC[X] <: Seq[X]](
      v1: CC[K],
      v2: CC[K]
    )(fudgers: Seq[
        (OpticDefns.RootLens[K] => Lens[K, Option[BigDecimal]],
            Tuple2[Option[BigDecimal], Option[BigDecimal]] => Tuple2[Option[
              BigDecimal
            ], Option[BigDecimal]]
          )
      ]
    ): Prop =
    if (v1.size != v2.size)
      Prop.falsified :| {
        "Expected Seq of size " + v1.size + " but got " + v2.size
      }
    else {
      val together = v1.zip(v2)
      val m =
        together.map { p =>
          fudgers.foldLeft(p) { (curr, nf) =>
            val theLens = nf._1(lens[K])
            val p = (theLens.get(curr._1), theLens.get(curr._2))
            val (nl, nr) = nf._2(p)
            (theLens.set(curr._1)(nl), theLens.set(curr._2)(nr))
          }
        }.toMap

      m.keys.toVector ?= m.values.toVector
    }

  def compareMaps[K](
      m1: Map[K, Option[BigDecimal]],
      m2: Map[K, Option[BigDecimal]],
      fudger: Tuple2[Option[BigDecimal], Option[BigDecimal]] => Tuple2[Option[
        BigDecimal
      ], Option[BigDecimal]]
    ): Prop = {
    def compareKey(k: K): Prop = {
      val m1v = m1.get(k)
      val m2v = m2.get(k)
      if (!m2v.isDefined)
        Prop.falsified :| {
          val expKey = Pretty.pretty[K](k, Pretty.Params(0))
          "Expected key of " + expKey + " in right side map"
        }
      else {
        val (v1, v2) = fudger((m1v.get, m2v.get))
        if (v1 == v2)
          Prop.proved
        else
          Prop.falsified :| {
            val expKey = Pretty.pretty[K](k, Pretty.Params(0))
            val leftVal =
              Pretty.pretty[Option[BigDecimal]](v1, Pretty.Params(0))
            val rightVal =
              Pretty.pretty[Option[BigDecimal]](v2, Pretty.Params(0))
            "For key of " + expKey + " expected " + leftVal + " got " + rightVal
          }
      }
    }

    if (m1.size != m2.size)
      Prop.falsified :| {
        "Expected map of size " + m1.size + " but got " + m2.size
      }
    else
      m1.keys.foldLeft(Prop.passed) { (curr, elem) => curr && compareKey(elem) }
  }

  /** running covar_pop and kurtosis multiple times is giving slightly different results */
  def tolerance(
      p: Tuple2[Option[BigDecimal], Option[BigDecimal]],
      of: BigDecimal
    ): Tuple2[Option[BigDecimal], Option[BigDecimal]] = {
    val comb = p._1.flatMap(a => p._2.map(b => (a, b)))
    if (comb.isEmpty)
      p
    else {
      val (l, r) = comb.get
      if ((l.max(r) - l.min(r)).abs < of)
        // tolerate it
        (Some(l), Some(l))
      else
        p
    }
  }

  import shapeless._

  def tl[X](
      lensf: OpticDefns.RootLens[X] => Lens[X, Option[BigDecimal]],
      of: BigDecimal
    ): (X, X) => (X, X) =
    (l: X, r: X) => {
      val theLens = lensf(lens[X])
      val (nl, rl) = tolerance((theLens.get(l), theLens.get(r)), of)
      (theLens.set(l)(nl), theLens.set(r)(rl))
    }

}

/** drop in conversion for doubles to handle serialization on cluster */
trait ToDecimal[A] {
  def truncate(a: A): Option[BigDecimal]

}

object ToDecimal {

  implicit val doubleToDecimal: ToDecimal[Double] = new ToDecimal[Double] {

    override def truncate(a: Double): Option[BigDecimal] =
      DoubleBehaviourUtils.nanNullHandler(a)
  }
}
