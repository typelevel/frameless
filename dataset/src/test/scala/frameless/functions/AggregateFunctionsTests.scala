package frameless
package functions

import java.math.MathContext

import frameless.functions.aggregate._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class AggregateFunctionsTests extends TypedDatasetSuite {

  def approximatelyEqual[A](a: A, b: A)(implicit numeric: Numeric[A]): Prop = {
    val da = numeric.toDouble(a)
    val db = numeric.toDouble(b)
    if((da.isNaN && db.isNaN) || (da.isInfinity && db.isInfinity)) proved
    else if ((da - db).abs < 1e-6) proved
    else falsified :| "Expected " + a + " but got " + b
  }

  test("sum") {
    def prop[A: TypedEncoder : Numeric : Summable](xs: List[A])(
      implicit
      eoa: TypedEncoder[Option[A]],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetSum = dataset.select(sum(A)).collect().run().toList

      datasetSum match {
        case x :: Nil => approximatelyEqual(x, xs.sum)
        case other => falsified
      }
    }

    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))

    // doesn't work yet because resulting type is different
    // check(forAll(prop[Int] _)
    // check(forAll(prop[Short] _)
    // check(forAll(prop[Byte] _)
  }

  test("avg") {
    def prop[A: TypedEncoder : Averagable](xs: List[A])(
      implicit
      fractional: Fractional[A],
      eoa: TypedEncoder[Option[A]],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val Vector(datasetAvg) = dataset.select(avg(A)).collect().run().toVector

      xs match {
        case Nil => datasetAvg ?= None
        case _ :: _ => datasetAvg match {
          case Some(x) => approximatelyEqual(x, fractional.div(xs.sum, fractional.fromInt(xs.size)))
          case other => falsified
        }
      }
    }

    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Double] _))
  }

  test("count") {
    def prop[A: TypedEncoder](xs: List[A]): Prop = {
      val dataset = TypedDataset.create(xs)
      val Vector(datasetCount) = dataset.select(count()).collect().run().toVector

      datasetCount ?= xs.size.toLong
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Byte] _))
  }

  test("count('a)") {
    def prop[A: TypedEncoder](xs: List[A])(implicit ex1: TypedEncoder[X1[A]]): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)
      val Vector(datasetCount) = dataset.select(count(A)).collect().run().toVector

      datasetCount ?= xs.size.toLong
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Byte] _))
  }
}
