package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.reflect.ClassTag

class SelectTests extends TypedDatasetSuite {
  test("select('a) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
      implicit
      ea: TypedEncoder[A],
      ex4: TypedEncoder[X4[A, B, C, D]],
      ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)

      val dataset2 = dataset.select(A).collect().run().toVector
      val data2 = data.map { case X4(a, _, _, _) => a }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
  }

  test("select('a, 'b) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
      implicit
      ea: TypedEncoder[A],
      eb: TypedEncoder[B],
      eab: TypedEncoder[(A, B)],
      ex4: TypedEncoder[X4[A, B, C, D]],
      ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)

      val dataset2 = dataset.select(A, B).collect().run().toVector
      val data2 = data.map { case X4(a, b, _, _) => (a, b) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, String, Int, Int] _))
  }

  test("select('a, 'b, 'c) FROM abcd") {
    def prop[A, B, C, D](data: Vector[X4[A, B, C, D]])(
      implicit
      ea: TypedEncoder[A],
      eb: TypedEncoder[B],
      ec: TypedEncoder[C],
      eab: TypedEncoder[(A, B, C)],
      ex4: TypedEncoder[X4[A, B, C, D]],
      ca: ClassTag[A]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val A = dataset.col[A]('a)
      val B = dataset.col[B]('b)
      val C = dataset.col[C]('c)

      val dataset2 = dataset.select(A, B, C).collect().run().toVector
      val data2 = data.map { case X4(a, b, c, _) => (a, b, c) }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[String, Int, Int, Int] _))
    check(forAll(prop[String, String, Int, Int] _))
  }

  test("select('a.b)") {
    def prop[A, B, C](data: Vector[X2[X2[A, B], C]])(
      implicit
      eabc: TypedEncoder[X2[X2[A, B], C]],
      eb: TypedEncoder[B],
      cb: ClassTag[B]
    ): Prop = {
      val dataset = TypedDataset.create(data)
      val AB = dataset.colMany('a, 'b)

      val dataset2 = dataset.select(AB).collect().run().toVector
      val data2 = data.map { case X2(X2(_, b), _) => b }

      dataset2 ?= data2
    }

    check(forAll(prop[Int, String, Double] _))
  }
}
