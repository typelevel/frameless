package frameless

import org.scalacheck.{Arbitrary, Prop}
import org.scalacheck.Prop._
import org.scalatest.Matchers

import scala.reflect.ClassTag
import shapeless.test.illTyped

class CreateTests extends TypedDatasetSuite with Matchers {

  import TypedEncoder.usingInjection

  test("creation using X4 derived DataFrames") {
    def prop[
    A: TypedEncoder,
    B: TypedEncoder,
    C: TypedEncoder,
    D: TypedEncoder](data: Vector[X4[A, B, C, D]]): Prop = {
      val ds = TypedDataset.create(data)
      TypedDataset.createUnsafe[X4[A, B, C, D]](ds.toDF()).collect().run() ?= data
    }

    check(forAll(prop[Int, Char, X2[Option[Country], Country], Int] _))
    check(forAll(prop[X2[Int, Int], Int, Boolean, Vector[Food]] _))
    check(forAll(prop[String, Food, X3[Food, Country, Boolean], Int] _))
    check(forAll(prop[
      Option[Vector[Food]],
      Vector[Vector[X2[Vector[(Person, X1[Char])], Country]]],
      X3[Food, Country, String],
      Vector[(Food, Country)]] _))
  }

  test("array fields") {
    def prop[T: Arbitrary: TypedEncoder: ClassTag] = forAll {
      (d1: Array[T], d2: Array[Option[T]], d3: Array[X1[T]], d4: Array[X1[Option[T]]],
        d5: X1[Array[T]]) =>
        TypedDataset.create(Seq(d1)).collect().run().head.sameElements(d1) &&
        TypedDataset.create(Seq(d2)).collect().run().head.sameElements(d2) &&
        TypedDataset.create(Seq(d3)).collect().run().head.sameElements(d3) &&
        TypedDataset.create(Seq(d4)).collect().run().head.sameElements(d4) &&
        TypedDataset.create(Seq(d5)).collect().run().head.a.sameElements(d5.a)
    }

    check(prop[Boolean])
    check(prop[Byte])
    check(prop[Short])
    check(prop[Int])
    check(prop[Long])
    check(prop[Float])
    check(prop[Double])
    check(prop[String])
  }

  test("vector fields") {
    def prop[T: Arbitrary: TypedEncoder] = forAll {
      (d1: Vector[T], d2: Vector[Option[T]], d3: Vector[X1[T]], d4: Vector[X1[Option[T]]],
        d5: X1[Vector[T]]) =>
      (TypedDataset.create(Seq(d1)).collect().run().head ?= d1) &&
      (TypedDataset.create(Seq(d2)).collect().run().head ?= d2) &&
      (TypedDataset.create(Seq(d3)).collect().run().head ?= d3) &&
      (TypedDataset.create(Seq(d4)).collect().run().head ?= d4) &&
      (TypedDataset.create(Seq(d5)).collect().run().head ?= d5)
    }

    check(prop[Boolean])
    check(prop[Byte])
    check(prop[Char])
    check(prop[Short])
    check(prop[Int])
    check(prop[Long])
    check(prop[Float])
    check(prop[Double])
    check(prop[String])
  }

  test("list fields") {
    def prop[T: Arbitrary: TypedEncoder] = forAll {
      (d1: List[T], d2: List[Option[T]], d3: List[X1[T]], d4: List[X1[Option[T]]],
        d5: X1[List[T]]) =>
      (TypedDataset.create(Seq(d1)).collect().run().head ?= d1) &&
        (TypedDataset.create(Seq(d2)).collect().run().head ?= d2) &&
        (TypedDataset.create(Seq(d3)).collect().run().head ?= d3) &&
        (TypedDataset.create(Seq(d4)).collect().run().head ?= d4) &&
        (TypedDataset.create(Seq(d5)).collect().run().head ?= d5)
    }

    check(prop[Boolean])
    check(prop[Byte])
    check(prop[Char])
    check(prop[Short])
    check(prop[Int])
    check(prop[Long])
    check(prop[Float])
    check(prop[Double])
    check(prop[String])
  }

  test("map fields (scala.Predef.Map / scala.collection.immutable.Map)") {
    def prop[A: Arbitrary: TypedEncoder, B: Arbitrary: TypedEncoder] = forAll {
      (d1: Map[A, B], d2: Map[B, A], d3: Map[A, Option[B]],
        d4: Map[A, X1[B]], d5: Map[X1[A], B], d6: Map[X1[A], X1[B]]) =>

      (TypedDataset.create(Seq(d1)).collect().run().head ?= d1) &&
      (TypedDataset.create(Seq(d2)).collect().run().head ?= d2) &&
      (TypedDataset.create(Seq(d3)).collect().run().head ?= d3) &&
      (TypedDataset.create(Seq(d4)).collect().run().head ?= d4) &&
      (TypedDataset.create(Seq(d5)).collect().run().head ?= d5) &&
      (TypedDataset.create(Seq(d6)).collect().run().head ?= d6)
    }

    check(prop[String, String])
    check(prop[String, Boolean])
    check(prop[String, Byte])
    check(prop[String, Char])
    check(prop[String, Short])
    check(prop[String, Int])
    check(prop[String, Long])
    check(prop[String, Float])
    check(prop[String, Double])
  }

  test("maps with Option keys should not resolve the TypedEncoder") {
    val data: Seq[Map[Option[Int], Int]] = Seq(Map(Some(5) -> 5))
    illTyped("TypedDataset.create(data)", ".*could not find implicit value for parameter encoder.*")
  }

  test("not aligned columns should throw an exception") {
    val v = Vector(X2(1,2))
    val df = TypedDataset.create(v).dataset.toDF()

    a [IllegalStateException] should be thrownBy {
      TypedDataset.createUnsafe[X1[Int]](df).show().run()
    }
  }

  test("dataset with different column order") {
    // e.g. when loading data from partitioned dataset
    // the partition columns get appended to the end of the underlying relation
    def prop[A: Arbitrary: TypedEncoder, B: Arbitrary: TypedEncoder] = forAll {
      (a1: A, b1: B) => {
        val ds = TypedDataset.create(
          Vector((b1, a1))
        ).dataset.toDF("b", "a").as[X2[A, B]](TypedExpressionEncoder[X2[A, B]])
        TypedDataset.create(ds).collect().run().head ?= X2(a1, b1)

      }
    }
    check(prop[X1[Double], X1[X1[SQLDate]]])
    check(prop[String, Int])
  }
}
