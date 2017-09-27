package frameless
package functions

import org.scalacheck.{ Arbitrary, Prop }
import org.scalacheck.Prop._
import scala.collection.SeqLike

import scala.math.Ordering

class UnaryFunctionsTest extends TypedDatasetSuite {
  test("size tests") {
    def prop[F[X] <: Traversable[X] : CatalystSizableCollection, A](xs: List[X1[F[A]]])(implicit arb: Arbitrary[F[A]], enc: TypedEncoder[F[A]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(size(tds('a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.size).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Vector, Long] _))
    check(forAll(prop[List, Long] _))
    check(forAll(prop[Vector, Char] _))
    check(forAll(prop[List, Char] _))
    check(forAll(prop[Vector, X2[Int, Option[Long]]] _))
    check(forAll(prop[List, X2[Int, Option[Long]]] _))
  }

  test("size on array test") {
    def prop[A: TypedEncoder](xs: List[X1[Array[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(size(tds('a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.size).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[String] _))
    check(forAll(prop[X2[Int, Option[Long]]] _))
  }

  test("sort in ascending order") {
    def prop[F[X] <: SeqLike[X, F[X]] : CatalystSortableCollection, A: Ordering](xs: List[X1[F[A]]])(implicit enc: TypedEncoder[F[A]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(sortAscending(tds('a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.sorted).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Vector, Long] _))
    check(forAll(prop[Vector, Int] _))
    check(forAll(prop[Vector, Char] _))
    check(forAll(prop[Vector, String] _))
    check(forAll(prop[List, Long] _))
    check(forAll(prop[List, Int] _))
    check(forAll(prop[List, Char] _))
    check(forAll(prop[List, String] _))
  }

  test("sort in descending order") {
    def prop[F[X] <: SeqLike[X, F[X]] : CatalystSortableCollection, A: Ordering](xs: List[X1[F[A]]])(implicit enc: TypedEncoder[F[A]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(sortDescending(tds('a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.sorted.reverse).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Vector, Long] _))
    check(forAll(prop[Vector, Int] _))
    check(forAll(prop[Vector, Char] _))
    check(forAll(prop[Vector, String] _))
    check(forAll(prop[List, Long] _))
    check(forAll(prop[List, Int] _))
    check(forAll(prop[List, Char] _))
    check(forAll(prop[List, String] _))
  }

  test("sort on array test: ascending order") {
    def prop[A: TypedEncoder : Ordering](xs: List[X1[Array[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(sortAscending(tds('a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.sorted).toVector

      Prop {
        framelessResults
          .zip(scalaResults)
          .forall {
            case (a, b) => a sameElements b
          }
      }
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("sort on array test: descending order") {
    def prop[A: TypedEncoder : Ordering](xs: List[X1[Array[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(sortDescending(tds('a))).collect().run().toVector
      val scalaResults = xs.map(x => x.a.sorted.reverse).toVector

      Prop {
        framelessResults
          .zip(scalaResults)
          .forall {
            case (a, b) => a sameElements b
          }
      }
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("explode on vectors") {
    def prop[F[X] <: Traversable[X] : CatalystExplodableCollection, A: TypedEncoder](xs: List[X1[F[A]]])(implicit arb: Arbitrary[F[A]], enc: TypedEncoder[F[A]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(explode(tds('a))).collect().run().toSet
      val scalaResults = xs.flatMap(_.a).toSet

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Vector, Long] _))
    check(forAll(prop[Vector, Int] _))
    check(forAll(prop[Vector, Char] _))
    check(forAll(prop[Vector, String] _))
    check(forAll(prop[List, Long] _))
    check(forAll(prop[List, Int] _))
    check(forAll(prop[List, Char] _))
    check(forAll(prop[List, String] _))
  }

  test("explode on arrays") {
    def prop[A: TypedEncoder](xs: List[X1[Array[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.select(explode(tds('a))).collect().run().toSet
      val scalaResults = xs.flatMap(_.a).toSet

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
