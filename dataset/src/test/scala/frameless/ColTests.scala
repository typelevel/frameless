package frameless

import shapeless.test.illTyped

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ColTests extends TypedDatasetSuite {
  test("col") {
    val x4 = TypedDataset.create[X4[Int, String, Long, Boolean]](Nil)
    val t4 = TypedDataset.create[(Int, String, Long, Boolean)](Nil)

    x4.col('a)
    t4.col('_1)

    x4.col[Int]('a)
    t4.col[Int]('_1)

    illTyped("x4.col[String]('a)", "No column .* of type String in frameless.X4.*")

    x4.col('b)
    t4.col('_2)

    x4.col[String]('b)
    t4.col[String]('_2)

    illTyped("x4.col[Int]('b)", "No column .* of type Int in frameless.X4.*")

    ()
  }

  test("colMany") {
    type X2X2 = X2[X2[Int, String], X2[Long, Boolean]]
    val x2x2 = TypedDataset.create[X2X2](Nil)

    val aa: TypedColumn[X2X2, Int] = x2x2.colMany('a, 'a)
    val ab: TypedColumn[X2X2, String] = x2x2.colMany('a, 'b)
    val ba: TypedColumn[X2X2, Long] = x2x2.colMany('b, 'a)
    val bb: TypedColumn[X2X2, Boolean] = x2x2.colMany('b, 'b)

    illTyped("x2x2.colMany('a, 'c)")
    illTyped("x2x2.colMany('a, 'a, 'a)")
  }

  test("select colMany") {
    def prop[A: TypedEncoder](x: X2[X2[A, A], A]): Prop = {
      val df = TypedDataset.create(x :: Nil)
      val got = df.select(df.colMany('a, 'a)).collect().run()

      got ?= (x.a.a :: Nil)
    }

    check(prop[Int] _)
    check(prop[X2[Int, Int]] _)
    check(prop[X2[X2[Int, Int], Int]] _)
  }
}
