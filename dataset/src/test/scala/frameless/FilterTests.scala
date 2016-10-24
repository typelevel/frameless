package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class FilterTests extends TypedDatasetSuite {
  test("filter") {
    def prop[A: TypedEncoder](elem: A, data: Vector[X1[A]])(implicit ex1: TypedEncoder[X1[A]]): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col('a)

      val dataset2 = dataset.filter(A === elem).collect().run().toVector
      val data2 = data.filter(_.a == elem)

      dataset2 ?= data2
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("filter with  arithmetic expressions: addition") {
    check(forAll { (data: Vector[X1[Int]]) =>
      val ds = TypedDataset.create(data)
      val res = ds.filter((ds('a) + 1) === (ds('a) + 1)).collect().run().toVector
      res ?= data
    })
  }

  test("filter with arithmetic expressions: multiplication") {
    val t = X1(1) :: X1(2) :: X1(3) :: Nil
    val tds: TypedDataset[X1[Int]] = TypedDataset.create(t)

    assert(tds.filter(tds('a) * 2 === 2).collect().run().toVector === Vector(X1(1)))
    assert(tds.filter(tds('a) * 3 === 3).collect().run().toVector === Vector(X1(1)))
  }
}
