package frameless

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop}

class FlatMapTests extends TypedDatasetSuite {

  // see issue with scalacheck non serializable Vector: https://github.com/rickynils/scalacheck/issues/315
  implicit def arbVector[A](implicit A: Arbitrary[A]): Arbitrary[Vector[A]] =
   Arbitrary(Gen.listOf(A.arbitrary).map(_.toVector))

  test("flatMap") {
    def prop[A: TypedEncoder, B: TypedEncoder](flatMapFunction: A => Vector[B], data: Vector[A]): Prop =
      TypedDataset.create(data).flatMap(flatMapFunction).collect().run().toVector =? data.flatMap(flatMapFunction)

    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Int, String] _))
    check(forAll(prop[String, Int] _))
  }
}
