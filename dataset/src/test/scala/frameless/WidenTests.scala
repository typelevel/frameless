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

  def widenSum[A: TypedEncoder: CatalystSummable: Numeric, B: TypedEncoder](a: A, b: B)(
    implicit
    view: B => A,
    colView: TypedColumn[X2[A, B], B] => TypedColumn[X2[A, B], A]
  ): Prop = {
    val df = TypedDataset.create(X2(a, b) :: Nil)
    val sum = implicitly[Numeric[A]].plus(a, view(b))

    val leftGot = df.select(df.col('a) plus df.col('b)).collect().run()
    val rightGot = df.select(df.col('b) plus df.col('a)).collect().run()

    (leftGot ?= (sum :: Nil)) &&
      (rightGot ?= (sum :: Nil))
  }


  {
    import frameless.implicits.widen._

    test("widen sum") {
      check(widenSum[Double, Int] _)
    }
  }

}
