package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalatest.Matchers

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

  test("not alligned columns should throw an exception") {
    val v = Vector(X2(1,2))
    val df = TypedDataset.create(v).dataset.toDF()

    a [IllegalStateException] should be thrownBy {
      TypedDataset.createUnsafe[X1[Int]](df).show().run()
    }
  }
}
