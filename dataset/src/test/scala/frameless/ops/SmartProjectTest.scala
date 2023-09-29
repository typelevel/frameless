package frameless
package ops

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless.test.illTyped


case class Foo(i: Int, j: Int, x: String)
case class Bar(i: Int, x: String)
case class InvalidFooProjectionType(i: Int, x: Boolean)
case class InvalidFooProjectionName(i: Int, xerr: String)

class SmartProjectTest extends TypedDatasetSuite {
  // Lazy needed to prevent initialization anterior to the `beforeAll` hook
  lazy val dataset = TypedDataset.create(Foo(1, 2, "hi") :: Foo(2, 3, "there") :: Nil)

  test("project Foo to Bar") {
    assert(dataset.project[Bar].count().run() === 2)
  }

  test("project to InvalidFooProjection should not type check") {
    illTyped("dataset.project[InvalidFooProjectionType]")
    illTyped("dataset.project[InvalidFooProjectionName]")
  }

  test("X4 to X1,X2,X3,X4 projections") {
    def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder, D: TypedEncoder](data: Vector[X4[A, B, C, D]]): Prop = {
      val dataset = TypedDataset.create(data)

      dataset.project[X4[A, B, C, D]].collect().run().toVector ?= data
      dataset.project[X3[A, B, C]].collect().run().toVector ?= data.map(x => X3(x.a, x.b, x.c))
      dataset.project[X2[A, B]].collect().run().toVector ?= data.map(x => X2(x.a, x.b))
      dataset.project[X1[A]].collect().run().toVector ?= data.map(x => X1(x.a))
    }

    check(forAll(prop[Int, String, X1[String], Boolean] _))
    check(forAll(prop[Short, Long, String, Boolean] _))
    check(forAll(prop[Short, (Boolean, Boolean), String, (Int, Int)] _))
    check(forAll(prop[X2[String, Boolean], (Boolean, Boolean), String, Boolean] _))
    check(forAll(prop[X2[String, Boolean], X3[Boolean, Boolean, Long], String, String] _))
  }

  test("X3U to X1,X2,X3 projections") {
    def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder](data: Vector[X3U[A, B, C]]): Prop = {
      val dataset = TypedDataset.create(data)

      dataset.project[X3[A, B, C]].collect().run().toVector ?= data.map(x => X3(x.a, x.b, x.c))
      dataset.project[X2[A, B]].collect().run().toVector ?= data.map(x => X2(x.a, x.b))
      dataset.project[X1[A]].collect().run().toVector ?= data.map(x => X1(x.a))
    }

    check(forAll(prop[Int, String, X1[String]] _))
    check(forAll(prop[Short, Long, String] _))
    check(forAll(prop[Short, (Boolean, Boolean), String] _))
    check(forAll(prop[X2[String, Boolean], (Boolean, Boolean), String] _))
    check(forAll(prop[X2[String, Boolean], X3[Boolean, Boolean, Long], String] _))
  }
}
