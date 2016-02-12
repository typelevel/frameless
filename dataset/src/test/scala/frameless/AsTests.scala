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

      val dataset2 = dataset.as[X2[A,B]]().collect().run.toVector
      val data2 = data.map { case (a, b) => X2(a, b) }

      dataset2 ?= data2
    }

    check(forAll { (xs: Vector[(Int, Int)]) => prop(xs) })
    check(forAll { (xs: Vector[(String, String)]) => prop(xs) })
    check(forAll { (xs: Vector[(String, Int)]) => prop(xs) })
    check(forAll { (xs: Vector[(Long, Int)]) => prop(xs) })
  }
}
