package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class UnionTests extends TypedDatasetSuite {
  test("Union") {
    def prop[A: TypedEncoder](data1: Vector[A], data2: Vector[A]): Prop = {
      val dataset1 = TypedDataset.create(data1)
      val dataset2 = TypedDataset.create(data2)
      val datasetUnion = dataset1.union(dataset2).collect().run().toVector
      val dataUnion = data1.union(data2)

      datasetUnion ?= dataUnion
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("align fields") {
    def prop[A: TypedEncoder, B: TypedEncoder](data1: Vector[(A, B)], data2: Vector[(A, B)]): Prop = {

      val dataset1 = TypedDataset.create(data1.map((Foo.apply[A, B] _).tupled))
      val dataset2 = TypedDataset.create(data2.map { case (a, b) => Bar[A, B](b, a) })
      val datasetUnion = dataset1.union(dataset2).collect().run().map(foo => (foo.x, foo.y)).toVector
      val dataUnion = data1.union(data2)

      datasetUnion ?= dataUnion
    }

    check(forAll(prop[Int, String] _))
    check(forAll(prop[String, Long] _))
  }
}

final case class Foo[A, B](x: A, y: B)

final case class Bar[A, B](y: B, x: A)