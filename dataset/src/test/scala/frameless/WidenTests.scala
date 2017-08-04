package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped

class WidenTests extends TypedDatasetSuite {

  // widening is off by default

  illTyped("""
     val df = TypedDataset.create(X2(1, 1.0))
     df.select(df('a) plus df('b))
  """)

  def widenSum[A: TypedEncoder: CatalystNumeric: Numeric, B: TypedEncoder](a: A, b: B)(
    implicit
    view: B => A,
    colView: TypedColumn[B] => TypedColumn[A]
  ): Prop = {
    val df = TypedDataset.create(X2(a, b) :: Nil)
    val sum = implicitly[Numeric[A]].plus(a, view(b))

    val leftGot = df.select(df.col('a) plus df.col('b)).collect().run()
    val rightGot = df.select(df.col('b) plus df.col('a)).collect().run()

    (leftGot ?= (sum :: Nil)) &&
      (rightGot ?= (sum :: Nil))
  }

  def widen[A: TypedEncoder, B: TypedEncoder](a: A)(
    implicit
    view: A => B,
    colView: TypedColumn[A] => TypedColumn[B]
  ): Prop = {
    val df = TypedDataset.create(X1(a) :: Nil)
    val got = df.select(colView(df.col('a))).collect().run()

    got ?= (view(a) :: Nil)
  }

  {
    import frameless.implicits.widen._

    test("widen sum") {
      check(widenSum[Double, Int] _)
    }

    test("widen") {
      implicit def byteToBigDecimal(x: Byte): BigDecimal = BigDecimal.valueOf(x.toLong)
      implicit def shortToBigDecimal(x: Short): BigDecimal = BigDecimal.valueOf(x.toLong)

      check(widen[Byte, Short] _)
      check(widen[Byte, Int] _)
      check(widen[Byte, Long] _)
      check(widen[Byte, Double] _)
      check(widen[Byte, BigDecimal] _)

      check(widen[Short, Int] _)
      check(widen[Short, Long] _)
      check(widen[Short, Double] _)
      check(widen[Short, BigDecimal] _)

      check(widen[Int, Long] _)
      check(widen[Int, Double] _)
      check(widen[Int, BigDecimal] _)

      check(widen[Long, Double] _)
      check(widen[Long, BigDecimal] _)

      check(widen[Double, BigDecimal] _)

      // this is lawful (or not?) when we have Float
      // check(widen[Byte, Float] _)
      // check(widen[Short, Float] _)
      // check(widen[Int, Float] _)
      // check(widen[Long, Float] _)
    }
  }

}
