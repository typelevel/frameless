package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped

class UnionTests extends TypedDatasetSuite {

  test("fail to compile on not aligned schema") {
    val dataset1 = TypedDataset.create(Foo(1, 1) :: Nil)
    val dataset2 = TypedDataset.create(Wrong(1, 1, 1) :: Nil)

    illTyped {
      """val fNew = dataset1 union dataset2 """
    }
  }

  test("Union for simple data types") {
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

  test("Align fields for case classes") {
    def prop[A: TypedEncoder, B: TypedEncoder](
        data1: Vector[(A, B)],
        data2: Vector[(A, B)]): Prop = {

      val dataset1 = TypedDataset.create(data1.map((Foo.apply[A, B] _).tupled))
      val dataset2 = TypedDataset.create(data2.map { case (a, b) => Bar[A, B](b, a) })
      val datasetUnion =
        dataset1.union(dataset2).collect().run().map(foo => (foo.x, foo.y)).toVector
      val dataUnion = data1 union data2

      datasetUnion ?= dataUnion
    }

    check(forAll(prop[Int, String] _))
    check(forAll(prop[String, X1[Option[Long]]] _))
  }

  test("Align fields for different number of columns") {
    def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](
        data1: Vector[(A, B, C)],
        data2: Vector[(A, B)]): Prop = {

      val dataset1 = TypedDataset.create(data2.map((Foo.apply[A, B] _).tupled))
      val dataset2 = TypedDataset.create(data1.map { case (a, b, c) => Baz[A, B, C](c, b, a) })
      val datasetUnion: Seq[(A, B)] =
        dataset1.union(dataset2).collect().run().map(foo => (foo.x, foo.y)).toVector
      val dataUnion = data2 union data1.map { case (a, b, _) => (a, b) }

      datasetUnion ?= dataUnion
    }

    check(forAll(prop[Option[Int], String, Array[Long]] _))
    check(forAll(prop[String, X1[Option[Int]], X2[String, Array[Int]]] _))
  }
}

final case class Foo[A, B](x: A, y: B)
final case class Bar[A, B](y: B, x: A)
final case class Baz[A, B, C](z: C, y: B, x: A)
final case class Wrong[A, B, C](a: A, b: B, c: C)
