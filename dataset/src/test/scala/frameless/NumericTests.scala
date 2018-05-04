package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import scala.reflect.ClassTag

class NumericTests extends TypedDatasetSuite {
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
    def prop[A: TypedEncoder : CatalystNumeric : Numeric : ClassTag](a: A, b: A): Prop = {
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
    def prop[A: TypedEncoder: CatalystNumeric: Numeric](a: A, b: A)(implicit cd: CatalystDivisible[A, Double]): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      if (b == 0) proved else {
        val div = implicitly[Numeric[A]].toDouble(a) / implicitly[Numeric[A]].toDouble(b)
        val got = df.select(df.col('a) / df.col('b)).collect().run()

        got ?= (div :: Nil)
      }
    }

    check(prop[Byte  ] _)
    check(prop[Double] _)
    check(prop[Int   ] _)
    check(prop[Long  ] _)
    check(prop[Short ] _)
  }

  test("divide BigDecimals") {
    def prop(a: BigDecimal, b: BigDecimal): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      if (b.doubleValue == 0) proved else {
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

  test("modulo") {
    def prop[A: TypedEncoder: CatalystIntegral: Integral](a: A, b: A): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      if (b == 0) proved else {
        val result = implicitly[Integral[A]].rem(a, b)
        val got = df.select(df.col('a) % df.col('b)).collect().run()

        got ?= (result :: Nil)
      }
    }

    check(prop[Byte] _)
    check(prop[Short] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Float] _)
    check(prop[Double] _)
    check(prop[BigDecimal] _)
  }
}
