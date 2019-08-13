package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop._


class FlattenTests extends TypedDatasetSuite {
  test("simple flatten test") {
    val ds: TypedDataset[(Int,Option[Int])] = TypedDataset.create(Seq((1,Option(1))))
    ds.flattenOption(Symbol("_2")): TypedDataset[(Int,Int)]
  }

  test("different Optional types") {
    def prop[A: TypedEncoder](xs: List[X1[Option[A]]]): Prop = {
      val tds: TypedDataset[X1[Option[A]]] = TypedDataset.create(xs)

      val framelessResults: Seq[Tuple1[A]] = tds.flattenOption(Symbol("a")).collect().run().toVector
      val scalaResults = xs.flatMap(_.a).map(Tuple1(_)).toVector

      framelessResults ?= scalaResults
    }

    check(forAll(prop[Long] _))
    check(forAll(prop[Int] _))
    check(forAll(prop[Char] _))
    check(forAll(prop[String] _))
  }
}
