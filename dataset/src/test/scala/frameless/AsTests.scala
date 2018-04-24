package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class AsTests extends TypedDatasetSuite {
  test("as[X2[A, B]]") {
    def prop[A, B](data: Vector[(A, B)])(
      implicit
      eab: TypedEncoder[(A, B)],
      ex2: TypedEncoder[X2[A, B]]
    ): Prop = {
      val dataset = TypedDataset.create(data)

      val dataset2 = dataset.as[X2[A,B]]().collect().run().toVector
      val data2 = data.map { case (a, b) => X2(a, b) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int] _))
    check(forAll(prop[String, String] _))
    check(forAll(prop[String, Int] _))
    check(forAll(prop[Long, Int] _))
    check(forAll(prop[Seq[Seq[Option[Seq[Long]]]], Seq[Int]] _))
    check(forAll(prop[Seq[Option[Seq[String]]], Seq[Int]] _))
  }

  test("as[X2[X2[A, B], C]") {
    def prop[A, B, C](data: Vector[(A, B, C)])(
      implicit
      eab: TypedEncoder[((A, B), C)],
      ex2: TypedEncoder[X2[X2[A, B], C]]
    ): Prop = {
      val data2 = data.map {
        case (a, b, c) => ((a, b), c)
      }
      val dataset = TypedDataset.create(data2)

      val dataset2 = dataset.as[X2[X2[A,B], C]]().collect().run().toVector
      val data3 = data2.map { case ((a, b), c) => X2(X2(a, b), c) }

      dataset2 ?= data3
    }

    check(forAll(prop[String, Int, Int] _))
    check(forAll(prop[String, Int, String] _))
    check(forAll(prop[String, String, Int] _))
    check(forAll(prop[Long, Int, String] _))
    check(forAll(prop[Seq[Seq[Option[Seq[Long]]]], Seq[Int], Option[Seq[Option[Int]]]] _))
    check(forAll(prop[Seq[Option[Seq[String]]], Seq[Int], Seq[Option[String]]] _))
  }
}
