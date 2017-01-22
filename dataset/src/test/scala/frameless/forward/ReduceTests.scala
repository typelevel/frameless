package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

class ReduceTests extends TypedDatasetSuite {
  def prop[A: TypedEncoder](reduceFunction: (A, A) => A)(data: Vector[A]): Prop =
    TypedDataset.create(data).reduceOption(reduceFunction).run() =? data.reduceOption(reduceFunction)

  test("reduce Int") {
    check(forAll(prop[Int](_ + _) _))
    check(forAll(prop[Int](_ * _) _))
  }

  test("reduce String") {
    def reduce(s1: String, s2: String): String = (s1 ++ s2).sorted
    check(forAll(prop[String](reduce) _))
  }
}
