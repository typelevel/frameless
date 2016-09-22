package frameless
package functions

import frameless.functions.aggregate._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class AggregateFunctionsTests extends TypedDatasetSuite {

  def approximatelyEqual[A](a: A, b: A)(implicit numeric: Numeric[A]): Prop = {
    val da = numeric.toDouble(a)
    val db = numeric.toDouble(b)
    // Spark has a weird behaviour concerning expressions that should return Inf
    // Most of the time they return NaN instead, for instance stddev of Seq(-7.827553978923477E227, -5.009124275715786E153)
    if((da.isNaN || da.isInfinity) && (db.isNaN || db.isInfinity)) proved
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
    def prop[A: TypedEncoder : Averageable](xs: List[A])(
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

  test("stddev") {

    def std[A](xs: List[A])(
      implicit
      fractional: Fractional[A],
      numeric: Numeric[A]
    ): Double = {
      val ds = xs.map(numeric.toDouble)
      val avg = ds.sum / ds.size
      math.sqrt((0.0 /: ds) {
        (a,e) => a + math.pow(e - avg, 2.0)
      } / (xs.size - 1))
    }

    def prop[A: TypedEncoder : Variance : Fractional : Numeric](xs: List[A])(
      implicit
      eoa: TypedEncoder[Option[A]],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val Vector(datasetStd) = dataset.select(stddev(A)).collect().run().toVector


      xs match {
        case Nil => datasetStd ?= None
        case _ :: Nil => datasetStd match {
          case Some(x) => if (implicitly[Numeric[A]].toDouble(x).isNaN) proved else falsified
          case _ => falsified
        }
        case _ => datasetStd match {
          case Some(x) => approximatelyEqual(implicitly[Numeric[A]].toDouble(x), std(xs))
          case _ => falsified
        }
      }
    }

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

  test("max") {
    def prop[A: TypedEncoder : Ordering](xs: List[A])(
      implicit
      ex1: TypedEncoder[X1[A]],
      eoa: TypedEncoder[Option[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)
      val datasetMax = dataset.select(max(A)).collect().run().toList.head

      xs match {
        case Nil => datasetMax.isEmpty
        case xs => datasetMax match {
          case Some(m) => xs.max ?= m
          case _ => falsified
        }
      }
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[String] _))
  }

  test("min") {
    def prop[A: TypedEncoder : Ordering](xs: List[A])(
      implicit
      ex1: TypedEncoder[X1[A]],
      eoa: TypedEncoder[Option[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetMin = dataset.select(min(A)).collect().run().toList.head
      xs match {
        case Nil => datasetMin.isEmpty
        case xs => datasetMin match {
          case Some(m) => xs.min ?= m
          case _ => falsified
        }
      }
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[String] _))
  }

  test("first") {
    def prop[A: TypedEncoder](xs: List[A])(
      implicit
      ex1: TypedEncoder[X1[A]],
      eoa: TypedEncoder[Option[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetFirst :: Nil = dataset.select(first(A)).collect().run().toList

      xs match {
        case Nil => datasetFirst.isEmpty
        case x::_ => datasetFirst match {
          case Some(m) => x ?= m
          case _ => falsified
        }
      }
    }

    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[String] _))
  }

  test("last") {
    def prop[A: TypedEncoder](xs: List[A])(
      implicit
      ex1: TypedEncoder[X1[A]],
      eoa: TypedEncoder[Option[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetLast :: Nil = dataset.select(last(A)).collect().run().toList

      xs match {
        case Nil => datasetLast.isEmpty
        case xs => datasetLast match {
          case Some(m) => xs.last ?= m
          case _ => falsified
        }
      }
    }

    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[String] _))
  }
}
