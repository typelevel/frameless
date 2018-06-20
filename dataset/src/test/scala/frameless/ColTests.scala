package frameless

import shapeless.test.illTyped

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ColTests extends TypedDatasetSuite {
  test("col") {
    type X4T = X4[Int, String, Long, Boolean]
    type T4 = (Int, String, Long, Boolean)
    val x4 = TypedDataset.create[X4T](Nil)
    val t4 = TypedDataset.create[T4](Nil)

    x4.col('a)
    t4.col('_1)

    x4.col[Int, X4T]('a)
    t4.col[Int, T4]('_1)

    illTyped("x4.col[String, X4T]('a)", "No column .* of type String in frameless.X4.*")

    x4.col('b)
    t4.col('_2)

    x4.col[String, X4T]('b)
    t4.col[String, T4]('_2)

    illTyped("x4.col[Int, X4T]('b)", "No column .* of type Int in frameless.X4.*")

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

  test("schema wrapper test: wrapper activated") {
    val t = TypedDataset.create(1 to 2)
    t.col[Int, Tuple1[Int]]('_1)
    ()
  }

  test("schema wrapper test: wrapper not activated") {
    val t = TypedDataset.create(Seq(X1(1)))
    t.col[Int, X1[Int]]('a)
    ()
  }
}
