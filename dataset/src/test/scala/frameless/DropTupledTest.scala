package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class DropTupledTest extends TypedDatasetSuite {
  test("drop five columns") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d5 = TypedDataset.create(X5(value, value, value, value, value) :: Nil)
      val d4 = d5.dropTupled(Symbol("a")) //drops first column
      val d3 = d4.dropTupled(Symbol("_4")) //drops last column
      val d2 = d3.dropTupled(Symbol("_2")) //drops middle column
      val d1 = d2.dropTupled(Symbol("_2"))

      Tuple1(value) ?= d1.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }

  test("drop first column") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d3 = TypedDataset.create(X3(value, value, value) :: Nil)
      val d2 = d3.dropTupled(Symbol("a"))

      (value, value) ?= d2.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }

  test("drop middle column") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d3 = TypedDataset.create(X3(value, value, value) :: Nil)
      val d2 = d3.dropTupled(Symbol("b"))

      (value, value) ?= d2.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }

  test("drop last column") {
    def prop[A: TypedEncoder](value: A): Prop = {
      val d3 = TypedDataset.create(X3(value, value, value) :: Nil)
      val d2 = d3.dropTupled(Symbol("c"))

      (value, value) ?= d2.collect().run().head
    }

    check(prop[Int] _)
    check(prop[Long] _)
    check(prop[String] _)
    check(prop[SQLDate] _)
    check(prop[Option[X1[Boolean]]] _)
  }
}
