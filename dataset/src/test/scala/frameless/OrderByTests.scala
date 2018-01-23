package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers
import shapeless.test.illTyped

class OrderByTests extends TypedDatasetSuite with Matchers {
  test("sorting single column descending") {
    def prop[A: TypedEncoder : CatalystOrdered](data: Vector[X1[A]]): Prop = {
      val ds = TypedDataset.create(data)

      ds.dataset.orderBy(ds.dataset.col("a").desc).collect().toVector.?=(
        ds.orderBy(ds('a).desc).collect().run().toVector)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("sorting single column ascending") {
    def prop[A: TypedEncoder : CatalystOrdered](data: Vector[X1[A]]): Prop = {
      val ds = TypedDataset.create(data)

      ds.dataset.orderBy(ds.dataset.col("a").asc).collect().toVector.?=(
        ds.orderBy(ds('a).asc).collect().run().toVector)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
    check(forAll(prop[Boolean] _))
  }

  test("sorting on two columns") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered](data: Vector[X2[A,B]]): Prop = {
      val ds = TypedDataset.create(data)

      val vanillaSpark = ds.dataset.orderBy(ds.dataset.col("a").asc, ds.dataset.col("b").desc).collect().toVector
      vanillaSpark.?=(ds.orderBy(ds('a).asc, ds('b).desc).collect().run().toVector).&&(
        vanillaSpark ?= ds.orderBy(ds('a).asc, ds('b).desc).collect().run().toVector
      )
    }

    check(forAll(prop[SQLDate, Long] _))
    check(forAll(prop[String, Boolean] _))
    check(forAll(prop[SQLTimestamp, Long] _))
  }

  test("sorting on three columns") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered]
    (data: Vector[X3[A, B, A]]): Prop = {
      val ds = TypedDataset.create(data)

      val vanillaSpark =  ds.dataset.orderBy(
        ds.dataset.col("a").desc,
        ds.dataset.col("b").desc,
        ds.dataset.col("c").asc
      ).collect().toVector

      vanillaSpark.?=(ds.orderBy(ds('a).desc, ds('b).desc, ds('c).asc).collect().run().toVector).&&(
        vanillaSpark ?= ds.orderBy(ds('a).desc, ds('b).desc, ds('c).asc).collect().run().toVector)
    }

    check(forAll(prop[Int, Long] _))
    check(forAll(prop[String, SQLDate] _))
    check(forAll(prop[Boolean, Long] _))
  }

  test("fail when selected column is not sortable") {
    val d = TypedDataset.create(X2(1, List(1)) :: X2(2, List(2)) :: Nil)
    d.orderBy(d('a).desc)
    illTyped("""d.orderByDesc('b)""")
    d.orderBy(d('a).desc)
    illTyped("""d.orderByMany(d('b).desc)""")
//    illTyped("""d.orderByMany(d('a))""") // column is correct, but no ordering is selected
  }
}