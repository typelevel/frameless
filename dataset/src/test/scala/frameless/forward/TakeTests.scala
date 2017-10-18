package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._
import scala.reflect.ClassTag

class TakeTests extends TypedDatasetSuite {
  test("take") {
    def prop[A: TypedEncoder](n: Int, data: Vector[A]): Prop =
      (n >= 0) ==> (TypedDataset.create(data).take(n).run().toVector =? data.take(n))

    def propArray[A: TypedEncoder: ClassTag](n: Int, data: Vector[X1[Array[A]]]): Prop =
      (n >= 0) ==> {
        Prop {
          TypedDataset.create(data).take(n).run().toVector.zip(data.take(n)).forall {
            case (X1(l), X1(r)) => l sameElements r
          }
        }
      }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
    check(forAll(propArray[Int] _))
    check(forAll(propArray[String] _))
    check(forAll(propArray[Byte] _))
  }
}
