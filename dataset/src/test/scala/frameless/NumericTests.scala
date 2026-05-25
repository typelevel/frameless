package frameless

import org.apache.spark.sql.Encoder
import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.scalacheck.Prop._
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

class NumericTests extends TypedDatasetSuite with Matchers {
  test("plus") {
    def prop[A: TypedEncoder: CatalystNumeric: Numeric](a: A, b: A): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = implicitly[Numeric[A]].plus(a, b)
      val got = df.select(df.col('a) + df.col('b)).collect().run()

      got ?= (result :: Nil)
    }

    check(prop[BigDecimal] _)
    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
  }

  test("minus") {
    def prop[A: TypedEncoder: CatalystNumeric: Numeric](a: A, b: A): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = implicitly[Numeric[A]].minus(a, b)
      val got = df.select(df.col('a) - df.col('b)).collect().run()

      got ?= (result :: Nil)
    }

    check(prop[BigDecimal] _)
    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
  }

  test("multiply") {
    def prop[A: TypedEncoder: CatalystNumeric: Numeric: ClassTag](
        a: A,
        b: A
      ): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = implicitly[Numeric[A]].times(a, b)
      val got = df.select(df.col('a) * df.col('b)).collect().run()

      got ?= (result :: Nil)
    }

    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
  }

  test("divide") {
    def prop[A: TypedEncoder: CatalystNumeric: Numeric](
        a: A,
        b: A
      )(implicit
        cd: CatalystDivisible[A, Double]
      ): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      if (b == 0) proved
      else {
        val div: Double = implicitly[Numeric[A]]
          .toDouble(a) / implicitly[Numeric[A]].toDouble(b)
        val got: Seq[Double] =
          df.select(df.col('a) / df.col('b)).collect().run()

        got ?= (div :: Nil)
      }
    }

    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
  }

  test("divide BigDecimals") {
    def prop(a: BigDecimal, b: BigDecimal): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      if (b.doubleValue == 0) proved
      else {
        // Spark performs something in between Double division and BigDecimal division,
        // we approximate it using double vision and `approximatelyEqual`:
        val div = BigDecimal(a.doubleValue / b.doubleValue)
        val got = df.select(df.col('a) / df.col('b)).collect().run()
        approximatelyEqual(got.head, div)
      }
    }

    check(prop _)
  }

  test("multiply BigDecimal") {
    def prop(a: BigDecimal, b: BigDecimal): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val result = BigDecimal(a.doubleValue * b.doubleValue)
      val got = df.select(df.col('a) * df.col('b)).collect().run()
      approximatelyEqual(got.head, result)
    }

    check(prop _)
  }

  trait NumericMod[T] {
    def mod(a: T, b: T): T
  }

  object NumericMod {

    implicit val byteInstance = new NumericMod[Byte] {
      def mod(a: Byte, b: Byte) = (a % b).toByte
    }

    implicit val doubleInstance = new NumericMod[Double] {
      def mod(a: Double, b: Double) = a % b
    }

    implicit val floatInstance = new NumericMod[Float] {
      def mod(a: Float, b: Float) = a % b
    }

    implicit val intInstance = new NumericMod[Int] {
      def mod(a: Int, b: Int) = a % b
    }

    implicit val longInstance = new NumericMod[Long] {
      def mod(a: Long, b: Long) = a % b
    }

    implicit val shortInstance = new NumericMod[Short] {
      def mod(a: Short, b: Short) = (a % b).toShort
    }

    implicit val bigDecimalInstance = new NumericMod[BigDecimal] {
      def mod(a: BigDecimal, b: BigDecimal) = a % b
    }
  }

  test("mod") {
    import NumericMod._

    def prop[A: TypedEncoder: CatalystNumeric: NumericMod](a: A, b: A): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      if (b == 0) proved
      else {
        val mod: A = implicitly[NumericMod[A]].mod(a, b)
        val got: Seq[A] = df.select(df.col('a) % df.col('b)).collect().run()

        got ?= (mod :: Nil)
      }
    }

    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
    check(prop[BigDecimal] _)
  }

  test("a mod lit(b)") {
    import NumericMod._

    def prop[A: TypedEncoder: CatalystNumeric: NumericMod](
        elem: A,
        data: X1[A]
      ): Prop = {
      val dataset = TypedDataset.create(Seq(data))
      val a = dataset.col('a)
      if (elem == 0) proved
      else {
        val mod: A = implicitly[NumericMod[A]].mod(data.a, elem)
        val got: Seq[A] = dataset.select(a % elem).collect().run()

        got ?= (mod :: Nil)
      }
    }

    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
    check(prop[BigDecimal] _)
  }

  test("isNaN") {
    val spark = session
    import spark.implicits._

    implicit val doubleWithNaN = Arbitrary {
      implicitly[Arbitrary[Double]].arbitrary.flatMap(Gen.oneOf(_, Double.NaN))
    }
    implicit val x1 = Arbitrary { doubleWithNaN.arbitrary.map(X1(_)) }

    def prop[A: TypedEncoder: Encoder: CatalystNaN](data: List[X1[A]]): Prop = {
      val ds = TypedDataset.create(data)

      val expected =
        ds.toDF().filter(!$"a".isNaN).map(_.getAs[A](0)).collect().toSeq
      val rs = ds.filter(!ds('a).isNaN).collect().run().map(_.a)

      rs ?= expected
    }

    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
  }

  test("isNaN with non-nan types should not compile") {
    val ds = TypedDataset.create((1, false, 'a, "b") :: Nil)

    "ds.filter(ds('_1).isNaN)" shouldNot typeCheck
    "ds.filter(ds('_2).isNaN)" shouldNot typeCheck
    "ds.filter(ds('_3).isNaN)" shouldNot typeCheck
    "ds.filter(ds('_4).isNaN)" shouldNot typeCheck
  }
}
