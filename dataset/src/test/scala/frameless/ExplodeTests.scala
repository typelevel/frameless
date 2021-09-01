package frameless

import scala.reflect.ClassTag

import frameless.functions.CatalystExplodableCollection

import org.scalacheck.{Arbitrary, Prop}
import org.scalacheck.Prop.{forAll, _}

class ExplodeTests extends TypedDatasetSuite {
  test("simple explode test") {
    val ds = TypedDataset.create(Seq((1, Array(1, 2))))
    ds.explode('_2): TypedDataset[(Int, Int)]
  }

  test("explode on vectors/list/seq") {
    def prop[F[X] <: Traversable[X]: CatalystExplodableCollection, A: TypedEncoder](
        xs: List[X1[F[A]]])(implicit arb: Arbitrary[F[A]], enc: TypedEncoder[F[A]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.explode('a).collect().run().toVector
      val scalaResults = xs.flatMap(_.a).map(Tuple1(_)).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Vector, Long] _))
    check(forAll(prop[Seq, Int] _))
    check(forAll(prop[Vector, Char] _))
    check(forAll(prop[Vector, String] _))
    check(forAll(prop[List, Long] _))
    check(forAll(prop[List, Int] _))
    check(forAll(prop[List, Char] _))
    check(forAll(prop[List, String] _))
  }

  test("explode on arrays") {
    def prop[A: TypedEncoder: ClassTag](xs: List[X1[Array[A]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.explode('a).collect().run().toVector
      val scalaResults = xs.flatMap(_.a).map(Tuple1(_)).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
