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

  def sparkSchema[A: TypedEncoder, U](f: TypedColumn[X1[A], A] => TypedAggregate[X1[A], U]): Prop = {
    val df = TypedDataset.create[X1[A]](Nil)
    val col = f(df.col('a))

    val sumDf = df.agg(col)

    TypedExpressionEncoder.targetStructType(sumDf.encoder) ?= sumDf.dataset.schema
  }

  test("sum") {
    case class Sum4Tests[A, B](sum: Seq[A] => B)

    def prop[A: TypedEncoder, Out: TypedEncoder : Numeric](xs: List[A])(
      implicit
      summable: CatalystSummable[A, Out],
      summer: Sum4Tests[A, Out]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetSum: List[Out] = dataset.agg(sum(A)).collect().run().toList

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

    check(sparkSchema[BigDecimal, BigDecimal](sum))
    check(sparkSchema[Long, Long](sum))
    check(sparkSchema[Int, Long](sum))
    check(sparkSchema[Double, Double](sum))
    check(sparkSchema[Short, Long](sum))
  }

  test("avg") {
    case class Averager4Tests[A, B](avg: Seq[A] => B)

    def prop[A: TypedEncoder, Out: TypedEncoder : Numeric](xs: List[A])(
      implicit
      averageable: CatalystAverageable[A, Out],
      averager: Averager4Tests[A, Out]
    ): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetAvg: Vector[Out] = dataset.agg(avg(A)).collect().run().toVector

      if (datasetAvg.size > 2) falsified
      else xs match {
        case Nil => datasetAvg ?= Vector()
        case _ :: _ => datasetAvg.headOption match {
          case Some(x) => approximatelyEqual(averager.avg(xs), x)
          case None => falsified
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
    def prop[A: TypedEncoder : CatalystVariance : Numeric](xs: List[A]): Prop = {
      val numeric = implicitly[Numeric[A]]
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetStd = dataset.agg(stddev(A)).collect().run().toVector
      val std = sc.parallelize(xs.map(numeric.toDouble)).sampleStdev()

      if (datasetStd.size > 2) falsified
      else xs match {
        case Nil => datasetStd ?= Vector()
        case _ :: Nil => datasetStd.headOption match {
          case Some(x) => if (x.isNaN) proved else falsified
          case _ => falsified
        }
        case _ => datasetStd.headOption match {
          case Some(x) => approximatelyEqual(std, x)
          case _ => falsified
        }
      }
    }

    check(forAll(prop[Short] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Double] _))
  }

  test("count") {
    def prop[A: TypedEncoder](xs: List[A]): Prop = {
      val dataset = TypedDataset.create(xs)
      val Vector(datasetCount) = dataset.agg(count()).collect().run().toVector

      datasetCount ?= xs.size.toLong
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Byte] _))
  }

  test("count('a)") {
    def prop[A: TypedEncoder](xs: List[A]): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)
      val datasetCount = dataset.agg(count(A)).collect().run()

      datasetCount ?= List(xs.size.toLong)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Byte] _))
  }

  test("max") {
    def prop[A: TypedEncoder: CatalystOrdered](xs: List[A])(implicit o: Ordering[A]): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)
      val datasetMax = dataset.agg(max(A)).collect().run().toList

      datasetMax ?= xs.reduceOption(o.max).toList
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[String] _))
  }

  test("min") {
    def prop[A: TypedEncoder: CatalystOrdered](xs: List[A])(implicit o: Ordering[A]): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetMin = dataset.agg(min(A)).collect().run().toList

      datasetMin ?= xs.reduceOption(o.min).toList
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[String] _))
  }

  test("first") {
    def prop[A: TypedEncoder](xs: List[A]): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetFirst = dataset.agg(first(A)).collect().run().toList

      datasetFirst ?= xs.headOption.toList
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
    def prop[A: TypedEncoder](xs: List[A]): Prop = {
      val dataset = TypedDataset.create(xs.map(X1(_)))
      val A = dataset.col[A]('a)

      val datasetLast = dataset.agg(last(A)).collect().run().toList

      datasetLast ?= xs.lastOption.toList
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
