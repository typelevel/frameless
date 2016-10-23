package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

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
    def prop[A: TypedEncoder: CatalystNumeric: Numeric](a: A, b: A): Prop = {
      val df = TypedDataset.create(X2(a, b) :: Nil)
      val sum = implicitly[Numeric[A]].times(a, b)
      val got = df.select(df.col('a) * df.col('b)).collect().run()

      got ?= (sum :: Nil)
    }

    // FIXME doesn't work ¯\_(ツ)_/¯
    // check(prop[BigDecimal] _)
    check(prop[Byte] _)
    check(prop[Double] _)
    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[Short] _)
  }
}
