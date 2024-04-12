package frameless
package ops

import frameless.functions.aggregate._
import org.apache.spark.sql.{ functions => sparkFunctions }
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._
import org.scalacheck.{ Gen, Prop }

class PivotTest extends TypedDatasetSuite {

  def withCustomGenX4: Gen[Vector[X4[String, String, Int, Boolean]]] = {
    val kvPairGen: Gen[X4[String, String, Int, Boolean]] = for {
      a <- Gen.oneOf(Seq("1", "2", "3", "4"))
      b <- Gen.oneOf(Seq("a", "b", "c"))
      c <- arbitrary[Int]
      d <- arbitrary[Boolean]
    } yield X4(a, b, c, d)

    Gen.listOfN(4, kvPairGen).map(_.toVector)
  }

  test("X4[Boolean, String, Int, Boolean] pivot on String") {
    def prop(data: Vector[X4[String, String, Int, Boolean]]): Prop = {
      val d = TypedDataset.create(data)
      val frameless = d
        .coalesce(1)
        .orderBy(d('a).asc, d('d).asc)
        .groupBy(d('a))
        .pivot(d('b))
        .on("a", "b", "c")
        .agg(sum(d('c)), first(d('d)))
        .collect()
        .run()
        .toVector

      val spark = d.dataset
        .coalesce(1)
        .orderBy("a", "d")
        .groupBy("a")
        .pivot("b", Seq("a", "b", "c"))
        .agg(sparkFunctions.sum("c"), sparkFunctions.first("d"))
        .collect()
        .toVector

      (frameless.map(_._1) ?= spark.map(x => x.getAs[String](0)))
        .&&(frameless.map(_._2) ?= spark.map(x => Option(x.getAs[Long](1))))
        .&&(frameless.map(_._3) ?= spark.map(x => Option(x.getAs[Boolean](2))))
        .&&(frameless.map(_._4) ?= spark.map(x => Option(x.getAs[Long](3))))
        .&&(frameless.map(_._5) ?= spark.map(x => Option(x.getAs[Boolean](4))))
        .&&(frameless.map(_._6) ?= spark.map(x => Option(x.getAs[Long](5))))
        .&&(frameless.map(_._7) ?= spark.map(x => Option(x.getAs[Boolean](6))))
    }

    check(forAll(withCustomGenX4)(prop))
  }

  test("Pivot on Boolean") {
    val x: Seq[X3[String, Boolean, Boolean]] =
      Seq(X3("a", true, true), X3("a", true, true), X3("a", true, false))
    val d = TypedDataset.create(x)
    d.groupByMany(d('a))
      .pivot(d('c))
      .on(true, false)
      .agg(count[X3[String, Boolean, Boolean]]())
      .collect()
      .run()
      .toVector ?= Vector(("a", Some(2L), Some(1L))) // two true one false
  }

  test("Pivot with groupBy on two columns, pivot on Long") {
    val x: Seq[X3[String, String, Long]] =
      Seq(X3("a", "x", 1), X3("a", "x", 1), X3("a", "c", 20))
    val d = TypedDataset.create(x)
    d.groupBy(d('a), d('b))
      .pivot(d('c))
      .on(1L, 20L)
      .agg(count[X3[String, String, Long]]())
      .collect()
      .run()
      .toSet ?= Set(("a", "x", Some(2L), None), ("a", "c", None, Some(1L)))
  }

  test("Pivot with cube on two columns, pivot on Long") {
    val x: Seq[X3[String, String, Long]] =
      Seq(X3("a", "x", 1), X3("a", "x", 1), X3("a", "c", 20))
    val d = TypedDataset.create(x)
    d.cube(d('a), d('b))
      .pivot(d('c))
      .on(1L, 20L)
      .agg(count[X3[String, String, Long]]())
      .collect()
      .run()
      .toSet ?= Set(("a", "x", Some(2L), None), ("a", "c", None, Some(1L)))
  }

  test("Pivot with cube on Boolean") {
    val x: Seq[X3[String, Boolean, Boolean]] =
      Seq(X3("a", true, true), X3("a", true, true), X3("a", true, false))
    val d = TypedDataset.create(x)
    d.cube(d('a))
      .pivot(d('c))
      .on(true, false)
      .agg(count[X3[String, Boolean, Boolean]]())
      .collect()
      .run()
      .toVector ?= Vector(("a", Some(2L), Some(1L)))
  }

  test("Pivot with rollup on two columns, pivot on Long") {
    val x: Seq[X3[String, String, Long]] =
      Seq(X3("a", "x", 1), X3("a", "x", 1), X3("a", "c", 20))
    val d = TypedDataset.create(x)
    d.rollup(d('a), d('b))
      .pivot(d('c))
      .on(1L, 20L)
      .agg(count[X3[String, String, Long]]())
      .collect()
      .run()
      .toSet ?= Set(("a", "x", Some(2L), None), ("a", "c", None, Some(1L)))
  }

  test("Pivot with rollup on Boolean") {
    val x: Seq[X3[String, Boolean, Boolean]] =
      Seq(X3("a", true, true), X3("a", true, true), X3("a", true, false))
    val d = TypedDataset.create(x)
    d.rollupMany(d('a))
      .pivot(d('c))
      .on(true, false)
      .agg(count[X3[String, Boolean, Boolean]]())
      .collect()
      .run()
      .toVector ?= Vector(("a", Some(2L), Some(1L)))
  }
}
