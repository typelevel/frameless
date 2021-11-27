package frameless

import frameless.functions.CatalystExplodableCollection
import org.scalacheck.{Arbitrary, Prop}
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop._

import scala.reflect.ClassTag

class ExplodeTests extends TypedDatasetSuite {
  test("simple explode test") {
    val ds = TypedDataset.create(Seq((1,Array(1,2))))
    ds.explode('_2): TypedDataset[(Int,Int)]
  }

  test("explode on vectors/list/seq") {
    def prop[F[X] <: Traversable[X] : CatalystExplodableCollection, A: TypedEncoder](xs: List[X1[F[A]]])(implicit arb: Arbitrary[F[A]], enc: TypedEncoder[F[A]]): Prop = {
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

  test("explode on maps") {
    def prop[A: TypedEncoder: ClassTag, B: TypedEncoder: ClassTag](xs: List[X1[Map[A, B]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.explodeMap('a).collect().run().toVector
      val scalaResults = xs.flatMap(_.a.toList).map(t => Tuple1(Tuple2(t._1, t._2))).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long, String] _))
    check(forAll(prop[Int, Long] _))
    check(forAll(prop[String, Int] _))
  }

  test("explode on maps preserving other columns") {
    def prop[K: TypedEncoder: ClassTag, A: TypedEncoder: ClassTag, B: TypedEncoder: ClassTag](xs: List[X2[K, Map[A, B]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.explodeMap('b).collect().run().toVector
      val scalaResults = xs.flatMap { x2 => x2.b.toList.map((x2.a, _)) }.toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Int, Long, String] _))
    check(forAll(prop[String, Int, Long] _))
    check(forAll(prop[Long, String, Int] _))
  }

  test("explode on maps making sure no key / value naming collision happens") {
    def prop[K: TypedEncoder: ClassTag, V: TypedEncoder: ClassTag, A: TypedEncoder: ClassTag, B: TypedEncoder: ClassTag](xs: List[X3KV[K, V, Map[A, B]]]): Prop = {
      val tds = TypedDataset.create(xs)

      val framelessResults = tds.explodeMap('c).collect().run().toVector
      val scalaResults = xs.flatMap { x3 => x3.c.toList.map((x3.key, x3.value, _)) }.toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[String, Int, Long, String] _))
    check(forAll(prop[Long, String, Int, Long] _))
    check(forAll(prop[Int, Long, String, Int] _))
  }
}
