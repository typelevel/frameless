package frameless

import org.scalatest.matchers.should.Matchers
import shapeless.test.illTyped

case class MyClass1(a: Int, b: String, c: MyClass2, g: Option[MyClass4])
case class MyClass2(d: Long, e: MyClass3)
case class MyClass3(f: Double)
case class MyClass4(h: Boolean)

final class ColumnViaLambdaTests extends TypedDatasetSuite with Matchers {

  def ds = {
    TypedDataset.create(
      Seq(
        MyClass1(1, "2", MyClass2(3L, MyClass3(7.0d)), Some(MyClass4(true))),
        MyClass1(4, "5", MyClass2(6L, MyClass3(8.0d)), None)))
  }

  test("col(_.a)") {
    val col = TypedColumn[MyClass1, Int](_.a)

    ds.select(col).collect.run() shouldEqual Seq(1, 4)
  }

  test("col(x => x.a") {
    val col = TypedColumn[MyClass1, Int](x => x.a)

    ds.select(col).collect.run() shouldEqual Seq(1, 4)
  }

  test("col((x: MyClass1) => x.a") {
    val col = TypedColumn { (x: MyClass1) => x.a }

    ds.select(col).collect.run() shouldEqual Seq(1, 4)
  }

  test("col((x: MyClass1) => x.c.e.f") {
    val col = TypedColumn { (x: MyClass1) => x.c.e.f }

    ds.select(col).collect.run() shouldEqual Seq(7.0d, 8.0d)
  }

  test("col(_.c.d)") {
    val col = TypedColumn[MyClass1, Long](_.c.d)

    ds.select(col).collect.run() shouldEqual Seq(3L, 6L)
  }

  test("col(_.c.e.f)") {
    val col = TypedColumn[MyClass1, Double](_.c.e.f)

    ds.select(col).collect.run() shouldEqual Seq(7.0d, 8.0d)
  }

  test("col(_.c.d) as int does not compile (is long)") {
    illTyped("TypedColumn[MyClass1, Int](_.c.d)")
  }

  test("col(_.g.h does not compile") {
    val col = ds.col(_.g) // the path "ends" at .g (can't access h)
    illTyped("""ds.col(_.g.h)""")
  }

  test("col(_.a.toString) does not compile") {
    illTyped("""ds.col(_.a.toString)""")
  }

  test("col(_.a.toString.size) does not compile") {
    illTyped("""ds.col(_.a.toString.size)""")
  }

  test("col((x: MyClass1) => x.toString.size) does not compile") {
    illTyped("""ds.col((x: MyClass1) => x.toString.size)""")
  }

  test("col(x => java.lang.Math.abs(x.a)) does not compile") {
    illTyped("""col(x => java.lang.Math.abs(x.a))""")
  }
}
