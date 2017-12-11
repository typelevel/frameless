package frameless

import org.apache.spark.sql.{ functions => sfunc }
import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped

class SortTests extends TypedDatasetSuite {
  test("prevent sorting by Map") {
    val ds = TypedDataset.create(Seq(
      X2(1, Map.empty[String, Int])
    ))

    illTyped {
      """ds.sort(ds('d).desc)"""
    }
  }

  test("sorting") {
    def prop[A: TypedEncoder : CatalystRowOrdered](values: List[A]): Prop = {
      val input: List[X2[Int, A]] = values.zipWithIndex.map { case (a, i) => X2(i, a) }

      val ds = TypedDataset.create(input)

      (ds.sort(ds('b)).collect().run().toList ?= ds.dataset.sort(sfunc.col("b")).collect().toList) &&
        (ds.sort(ds('b).asc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc).collect().toList) &&
        (ds.sort(ds('b).desc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc).collect().toList)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
    check(prop[List[String]] _)
    check(prop[List[X2[Int, X1[String]]]] _)
  }

  test("sorting optional") {
    def prop[A: TypedEncoder : CatalystRowOrdered](values: List[Option[A]]): Prop = {
      val input: List[X2[Int, Option[A]]] = values.zipWithIndex.map { case (a, i) => X2(i, a) }

      val ds = TypedDataset.create(input)

      (ds.sort(ds('b)).collect().run().toList ?= ds.dataset.sort(sfunc.col("b")).collect().toList) &&
        (ds.sort(ds('b).asc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc).collect().toList) &&
        (ds.sort(ds('b).ascNonesFirst).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc_nulls_first).collect().toList) &&
        (ds.sort(ds('b).ascNonesLast).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").asc_nulls_last).collect().toList) &&
        (ds.sort(ds('b).desc).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc).collect().toList) &&
        (ds.sort(ds('b).descNonesFirst).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc_nulls_first).collect().toList) &&
        (ds.sort(ds('b).descNonesLast).collect().run().toList ?= ds.dataset.sort(sfunc.col("b").desc_nulls_last).collect().toList)
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
    check(prop[List[String]] _)
    check(prop[List[X2[Int, X1[String]]]] _)
  }
}
