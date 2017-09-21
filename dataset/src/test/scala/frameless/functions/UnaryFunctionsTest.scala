package frameless
package functions

import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.math.Ordering

class UnaryFunctionsTest extends TypedDatasetSuite {
  test("size on vector test") {
    def prop[A: TypedEncoder](xs: List[X1[Vector[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(size(tds(_.a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.size).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[X2[Int, Option[Long]]] _))
  }

  test("sort on vector test: ascending order") {
    def prop[A: TypedEncoder : Ordering](xs: List[X1[Vector[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(sortAscending(tds(_.a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.sorted).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[String] _))
  }

  test("sort on vector test: descending order") {
    def prop[A: TypedEncoder : Ordering](xs: List[X1[Vector[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(sortDescending(tds(_.a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.sorted.reverse).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[String] _))
  }

  test("explode on vectors") {
    def prop[A: TypedEncoder](xs: List[X1[Vector[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(explode(tds(_.a))).collect().run().toSet
      val scalaResults = xs.flatMap(_.a).toSet

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[String] _))
  }
}