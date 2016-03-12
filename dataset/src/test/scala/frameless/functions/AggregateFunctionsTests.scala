package frameless
package functions

import java.math.MathContext

import frameless.X1
import frameless.functions.aggregate._

import org.scalacheck.Prop
import org.scalacheck.Prop._

class AggregateFunctionsTests extends TypedDatasetSuite {

  def approximatelyEqual[A](a: A, b: A)(implicit n: Numeric[A]): Prop = {
    val mc = new MathContext(4)
    if (BigDecimal(n.toDouble(a)).round(mc) == BigDecimal(n.toDouble(b)).round(mc)) proved
    else falsified :| "Expected " + a + " but got " + b
  }

  test("sum") {
    def prop[A](xs: List[A])(
      implicit
      ea: TypedEncoder[A],
      eoa: TypedEncoder[Option[A]],
      ex1: TypedEncoder[X1[A]],
      n: Numeric[A],
      s: Summable[A]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetSum = dataset.select(sum(A)).collect().run.toVector

      if (xs.isEmpty)
        datasetSum ?= Vector(n.fromInt(0))
      else
        datasetSum match {
          case Vector(x) => approximatelyEqual(x, xs.sum)
          case other => falsified
        }
    }

    check(forAll { (xs: List[BigDecimal]) => prop(xs) })
    check(forAll { (xs: List[Long]) => prop(xs) })
    check(forAll { (xs: List[Double]) => prop(xs) })

    // doesn't work yet because resulting type is different
    // check(forAll { (xs: List[Int]) => prop(xs) })
    // check(forAll { (xs: List[Short]) => prop(xs) })
    // check(forAll { (xs: List[Byte]) => prop(xs) })
  }

  test("avg") {
    def prop[A](xs: List[A])(
      implicit
      fractional: Fractional[A],
      averagable: Averagable[A],
      ea: TypedEncoder[A],
      eoa: TypedEncoder[Option[A]],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val Vector(datasetAvg) = dataset.select(avg(A)).collect().run.toVector

      xs match {
        case Nil => datasetAvg ?= None
        case _ :: _ => datasetAvg match {
          case Some(x) => approximatelyEqual(x, fractional.div(xs.sum, fractional.fromInt(xs.size)))
          case other => falsified
        }
      }
    }

    check(forAll { (xs: List[BigDecimal]) => prop(xs) })
    check(forAll { (xs: List[Double]) => prop(xs) })
  }

  test("count") {
    def prop[A: TypedEncoder](xs: List[A]): Prop = {
      val dataset = TypedDataset.create(xs)
      val Vector(datasetCount) = dataset.select(count()).collect().run.toVector

      datasetCount ?= xs.size.toLong
    }

    check(forAll { (xs: List[Int]) => prop(xs) })
    check(forAll { (xs: List[Byte]) => prop(xs) })
  }

  test("count('a)") {
    def prop[A](xs: List[A])(
      implicit
      ea: TypedEncoder[A],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)
      val Vector(datasetCount) = dataset.select(count(A)).collect().run.toVector

      datasetCount ?= xs.size.toLong
    }

    check(forAll { (xs: List[Int]) => prop(xs) })
    check(forAll { (xs: List[Byte]) => prop(xs) })
  }
}
