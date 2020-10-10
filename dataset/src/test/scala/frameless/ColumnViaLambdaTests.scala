package frameless

import org.scalatest.matchers.should.Matchers
import shapeless.test.illTyped

case class MyClass1(a: Int, b: String, c: MyClass2)
case class MyClass2(d: Long, e: MyClass3)
case class MyClass3(f: Double)

class ColumnViaLambdaTests extends TypedDatasetSuite with Matchers {

  def ds = TypedDataset.create(Seq(
    MyClass1(1, "2", MyClass2(3L, MyClass3(7.0))),
    MyClass1(4, "5", MyClass2(6L, MyClass3(8.0)))))

  test("col(_.a)") {
    val col = ds.col(_.a)
    val actual = ds.select(col).collect.run()
    val expected = Seq(1, 4)
    actual shouldEqual expected
  }

  test("col(x => x.a") {
    val col = ds.col(x => x.a)
    val actual = ds.select(col).collect.run()
    val expected = Seq(1, 4)
    actual shouldEqual expected
  }

  test("col((x: MyClass1) => x.a") {
    val col = ds.col((x: MyClass1) => x.a)
    val actual = ds.select(col).collect.run()
    val expected = Seq(1, 4)
    actual shouldEqual expected
  }

  test("col((x:MyClass1) => x.a") {
    val col = ds.col((x: MyClass1) => x.a)
    val actual = ds.select(col).collect.run()
    val expected = Seq(1, 4)
    actual shouldEqual expected
  }

  test("col(_.c.d)") {
    val col = ds.col(_.c.d)
    val actual = ds.select(col).collect.run()
    val expected = Seq(3, 6)
    actual shouldEqual expected
  }

  test("col(_.c.e.f)") {
    val col = ds.col(_.c.e.f)
    val actual = ds.select(col).collect.run()
    val expected = Seq(7.0, 8.0)
    actual shouldEqual expected
  }

  test("col(_.a.toString) should not compile") {
    illTyped("""ds.col(_.a.toString)""")
  }

  test("col(x => java.lang.Math.abs(x.a)) should not compile") {
    illTyped("""col(x => java.lang.Math.abs(x.a))""")
  }

}
