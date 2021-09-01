package frameless
package ops

import shapeless.{::, HNil}

import org.scalacheck.Prop
import org.scalacheck.Prop.forAll

class ColumnTypesTest extends TypedDatasetSuite {
  test("test summoning") {
    def prop[A: TypedEncoder, B: TypedEncoder, C: TypedEncoder, D: TypedEncoder](
        data: Vector[X4[A, B, C, D]]): Prop = {
      val d: TypedDataset[X4[A, B, C, D]] = TypedDataset.create(data)
      val hlist = d('a) :: d('b) :: d('c) :: d('d) :: HNil

      type TC[N] = TypedColumn[X4[A, B, C, D], N]

      type IN = TC[A] :: TC[B] :: TC[C] :: TC[D] :: HNil
      type OUT = A :: B :: C :: D :: HNil

      implicitly[ColumnTypes.Aux[X4[A, B, C, D], IN, OUT]]
      Prop.passed // successful compilation implies test correctness
    }

    check(forAll(prop[Int, String, X1[String], Boolean] _))
    check(forAll(prop[Vector[Int], Vector[Vector[String]], X1[String], Option[String]] _))
  }
}
