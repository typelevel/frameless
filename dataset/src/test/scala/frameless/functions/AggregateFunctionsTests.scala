package frameless
package functions

import frameless.functions.aggregate._
import org.scalacheck.Prop
import org.scalacheck.Prop._

class AggregateFunctionsTests extends TypedDatasetSuite {

  def approximatelyEqual[A](a: A, b: A)(implicit numeric: Numeric[A]): Prop = {
    val da = numeric.toDouble(a)
    val db = numeric.toDouble(b)
    val epsilon = 1E-6
    // Spark has a weird behaviour concerning expressions that should return Inf
    // Most of the time they return NaN instead, for instance stddev of Seq(-7.827553978923477E227, -5.009124275715786E153)
    if((da.isNaN || da.isInfinity) && (db.isNaN || db.isInfinity)) proved
    else if (
      (da - db).abs < epsilon ||
      (da - db).abs < da.abs / 100)
        proved
    else falsified :| s"Expected $a but got $b, which is more than 1% off and greater than epsilon = $epsilon."
  }

  test("sum") {
    case class Sum4Tests[A, B](sum: Seq[A] => B)

    def prop[A: TypedEncoder : Numeric, Out: TypedEncoder : Numeric](xs: List[A])(
      implicit
      summable: CatalystSummable[A, Out],
      summer: Sum4Tests[A, Out],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetSum: List[Out] = dataset.select(sum(A)).collect().run().toList

      datasetSum match {
        case x :: Nil => approximatelyEqual(summer.sum(xs), x)
        case other => falsified
      }
    }

    // Replicate Spark's behaviour : Ints and Shorts are cast to Long
    // https://github.com/apache/spark/blob/7eb2ca8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L37
    implicit def summerDecimal = Sum4Tests[BigDecimal, BigDecimal](_.sum)
    implicit def summerDouble = Sum4Tests[Double, Double](_.sum)
    implicit def summerLong = Sum4Tests[Long, Long](_.sum)
    implicit def summerInt = Sum4Tests[Int, Long](_.map(_.toLong).sum)
    implicit def summerShort = Sum4Tests[Short, Long](_.map(_.toLong).sum)

    check(forAll(prop[BigDecimal, BigDecimal] _))
    check(forAll(prop[Long, Long] _))
    check(forAll(prop[Double, Double] _))
    check(forAll(prop[Int, Long] _))
    check(forAll(prop[Short, Long] _))
  }

  test("avg") {
    case class  Averager4Tests[A: Numeric, B: Numeric](avg: Seq[A] => B)

    def prop[A: TypedEncoder : Numeric, Out: TypedEncoder : Numeric](xs: List[A])(
      implicit
      averageable: CatalystAverageable[A, Out],
      averager: Averager4Tests[A, Out],
      eob: TypedEncoder[Option[Out]],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val Vector(datasetAvg): Vector[Option[Out]] = dataset.select(avg(A)).collect().run().toVector

      xs match {
        case Nil => datasetAvg ?= None
        case _ :: _ => datasetAvg match {
          case Some(x) => approximatelyEqual(averager.avg(xs), x)
          case other => falsified
        }
      }
    }

    // Replicate Spark's behaviour : If the datatype isn't BigDecimal cast type to Double
    // https://github.com/apache/spark/blob/7eb2ca8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L50
    implicit def averageDecimal = Averager4Tests[BigDecimal, BigDecimal](as => as.sum/as.size)
    implicit def averageDouble = Averager4Tests[Double, Double](as => as.sum/as.size)
    implicit def averageLong = Averager4Tests[Long, Double](as => as.map(_.toDouble).sum/as.size)
    implicit def averageInt = Averager4Tests[Int, Double](as => as.map(_.toDouble).sum/as.size)
    implicit def averageShort = Averager4Tests[Short, Double](as => as.map(_.toDouble).sum/as.size)

    check(forAll(prop[BigDecimal, BigDecimal] _))
    check(forAll(prop[Double, Double] _))
    check(forAll(prop[Long, Double] _))
    check(forAll(prop[Int, Double] _))
    check(forAll(prop[Short, Double] _))
  }

  test("stddev") {

    def prop[A: TypedEncoder : Variance : Fractional : Numeric](xs: List[A])(
      implicit
      eoa: TypedEncoder[Option[A]],
      ex1: TypedEncoder[X1[A]]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val Vector(datasetStd) = dataset.select(stddev(A)).collect().run().toVector
      val std = sc.parallelize(xs.map(implicitly[Numeric[A]].toDouble)).sampleStdev()

      xs match {
        case Nil => datasetStd ?= None
        case _ :: Nil => datasetStd match {
          case Some(x) => if (implicitly[Numeric[A]].toDouble(x).isNaN) proved else falsified
          case _ => falsified
        }
        case _ => datasetStd match {
          case Some(x) => approximatelyEqual(std, implicitly[Numeric[A]].toDouble(x))
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
