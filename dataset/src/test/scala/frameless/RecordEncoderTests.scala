package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import shapeless._, shapeless.syntax.singleton._

class RecordEncoderTests extends TypedDatasetSuite {
  test("serialize and collect") {
    def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder, D: TypedEncoder](
      data: Vector[(A, B, C, D)]): Prop = {
      val records = data map {
        case (a, b, c, d) =>
          'a ->> a :: 'b ->> b :: 'c ->> c :: 'd ->> d :: HNil
      }
      val dataset = TypedDataset.create(records)

      dataset.collect().run().toVector ?= records
    }

    check(forAll(prop[Int, Int, Int, Int] _))
    check(forAll(prop[X2[Int, Int], Double, String, X3[Int, String, Boolean]] _))
  }
}
