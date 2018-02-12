package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers
import shapeless.test.illTyped
import org.apache.spark.sql.Column

class OrderByTests extends TypedDatasetSuite with Matchers {
  def sortings[A : CatalystOrdered, T]: Seq[(TypedColumn[T, A] => SortedTypedColumn[T, A], Column => Column)] = Seq(
    (_.desc, _.desc),
    (_.asc, _.asc),
    (t => t, t => t) //default ascending
  )

  test("single column non nullable orderBy") {
    def prop[A: TypedEncoder : CatalystOrdered](data: Vector[X1[A]]): Prop = {
      val ds = TypedDataset.create(data)

      sortings[A, X1[A]].map { case (typ, untyp) =>
        ds.dataset.orderBy(untyp(ds.dataset.col("a"))).collect().toVector.?=(
          ds.orderBy(typ(ds('a))).collect().run().toVector)
      }.reduce(_ && _)
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
  }

  test("single column non nullable partition sorting") {
    def prop[A: TypedEncoder : CatalystOrdered](data: Vector[X1[A]]): Prop = {
      val ds = TypedDataset.create(data)

      sortings[A, X1[A]].map { case (typ, untyp) =>
        ds.dataset.sortWithinPartitions(untyp(ds.dataset.col("a"))).collect().toVector.?=(
          ds.sortWithinPartitions(typ(ds('a))).collect().run().toVector)
      }.reduce(_ && _)
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
  }

  test("two columns non nullable orderBy") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered](data: Vector[X2[A,B]]): Prop = {
      val ds = TypedDataset.create(data)

      sortings[A, X2[A, B]].reverse.zip(sortings[B, X2[A, B]]).map { case ((typA, untypA), (typB, untypB)) =>
        val vanillaSpark = ds.dataset.orderBy(untypA(ds.dataset.col("a")), untypB(ds.dataset.col("b"))).collect().toVector
        vanillaSpark.?=(ds.orderBy(typA(ds('a)), typB(ds('b))).collect().run().toVector).&&(
          vanillaSpark ?= ds.orderByMany(typA(ds('a)), typB(ds('b))).collect().run().toVector
        )
      }.reduce(_ && _)
    }

    check(forAll(prop[SQLDate, Long] _))
    check(forAll(prop[String, Boolean] _))
    check(forAll(prop[SQLTimestamp, Long] _))
  }

  test("two columns non nullable partition sorting") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered](data: Vector[X2[A,B]]): Prop = {
      val ds = TypedDataset.create(data)

      sortings[A, X2[A, B]].reverse.zip(sortings[B, X2[A, B]]).map { case ((typA, untypA), (typB, untypB)) =>
        val vanillaSpark = ds.dataset.sortWithinPartitions(untypA(ds.dataset.col("a")), untypB(ds.dataset.col("b"))).collect().toVector
        vanillaSpark.?=(ds.sortWithinPartitions(typA(ds('a)), typB(ds('b))).collect().run().toVector).&&(
          vanillaSpark ?= ds.sortWithinPartitionsMany(typA(ds('a)), typB(ds('b))).collect().run().toVector
        )
      }.reduce(_ && _)
    }

    check(forAll(prop[SQLDate, Long] _))
    check(forAll(prop[String, Boolean] _))
    check(forAll(prop[SQLTimestamp, Long] _))
  }

  test("three columns non nullable orderBy") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered](data: Vector[X3[A,B,A]]): Prop = {
      val ds = TypedDataset.create(data)

      sortings[A, X3[A, B, A]].reverse
        .zip(sortings[B, X3[A, B, A]])
        .zip(sortings[A, X3[A, B, A]])
        .map { case (((typA, untypA), (typB, untypB)), (typA2, untypA2)) =>
          val vanillaSpark = ds.dataset
            .orderBy(untypA(ds.dataset.col("a")), untypB(ds.dataset.col("b")), untypA2(ds.dataset.col("c")))
            .collect().toVector

          vanillaSpark.?=(ds.orderBy(typA(ds('a)), typB(ds('b)), typA2(ds('c))).collect().run().toVector).&&(
            vanillaSpark ?= ds.orderByMany(typA(ds('a)), typB(ds('b)), typA2(ds('c))).collect().run().toVector
          )
        }.reduce(_ && _)
    }

    check(forAll(prop[SQLDate, Long] _))
    check(forAll(prop[String, Boolean] _))
    check(forAll(prop[SQLTimestamp, Long] _))
  }

  test("three columns non nullable partition sorting") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered](data: Vector[X3[A,B,A]]): Prop = {
      val ds = TypedDataset.create(data)

      sortings[A, X3[A, B, A]].reverse
        .zip(sortings[B, X3[A, B, A]])
        .zip(sortings[A, X3[A, B, A]])
        .map { case (((typA, untypA), (typB, untypB)), (typA2, untypA2)) =>
          val vanillaSpark = ds.dataset
            .sortWithinPartitions(untypA(ds.dataset.col("a")), untypB(ds.dataset.col("b")), untypA2(ds.dataset.col("c")))
            .collect().toVector

          vanillaSpark.?=(ds.sortWithinPartitions(typA(ds('a)), typB(ds('b)), typA2(ds('c))).collect().run().toVector).&&(
            vanillaSpark ?= ds.sortWithinPartitionsMany(typA(ds('a)), typB(ds('b)), typA2(ds('c))).collect().run().toVector
          )
        }.reduce(_ && _)
    }

    check(forAll(prop[SQLDate, Long] _))
    check(forAll(prop[String, Boolean] _))
    check(forAll(prop[SQLTimestamp, Long] _))
  }

  test("sort support for mixed default and explicit ordering") {
    def prop[A: TypedEncoder : CatalystOrdered, B: TypedEncoder : CatalystOrdered](data: Vector[X2[A, B]]): Prop = {
      val ds = TypedDataset.create(data)

      ds.dataset.orderBy(ds.dataset.col("a"), ds.dataset.col("b").desc).collect().toVector.?=(
        ds.orderByMany(ds('a), ds('b).desc).collect().run().toVector) &&
      ds.dataset.sortWithinPartitions(ds.dataset.col("a"), ds.dataset.col("b").desc).collect().toVector.?=(
        ds.sortWithinPartitionsMany(ds('a), ds('b).desc).collect().run().toVector)
    }

    check(forAll(prop[SQLDate, Long] _))
    check(forAll(prop[String, Boolean] _))
    check(forAll(prop[SQLTimestamp, Long] _))
  }

  test("fail when selected column is not sortable") {
    val d = TypedDataset.create(X2(1, Map(1 -> 2)) :: X2(2, Map(2 -> 2)) :: Nil)
    d.orderBy(d('a).desc)
    illTyped("""d.orderBy(d('b).desc)""")
    illTyped("""d.sortWithinPartitions(d('b).desc)""")
  }
}