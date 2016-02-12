package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.reflect.ClassTag

class CollectTests extends TypedDatasetSuite {
  test("collect()") {
    def prop[A: TypedEncoder: ClassTag](data: Vector[A]): Prop = {
      TypedDataset.create(data).collect().run.toVector ?= data
    }

    check(forAll { (xs: Vector[X2[Int, Int]]) => prop(xs) })
    check(forAll { (xs: Vector[X2[String, String]]) => prop(xs) })
    check(forAll { (xs: Vector[X2[String, Int]]) => prop(xs) })
    check(forAll { (xs: Vector[X2[Long, Int]]) => prop(xs) })

    check(forAll { (xs: Vector[X2[X2[Int, String], Boolean]]) => prop(xs) })

    check(forAll { (xs: Vector[Int]) => prop(xs) })
    check(forAll { (xs: Vector[Long]) => prop(xs) })
    check(forAll { (xs: Vector[Double]) => prop(xs) })
    check(forAll { (xs: Vector[Float]) => prop(xs) })
    check(forAll { (xs: Vector[Short]) => prop(xs) })
    check(forAll { (xs: Vector[Byte]) => prop(xs) })
    check(forAll { (xs: Vector[Boolean]) => prop(xs) })
    check(forAll { (xs: Vector[String]) => prop(xs) })
    check(forAll { (xs: Vector[SQLDate]) => prop(xs) })
    check(forAll { (xs: Vector[SQLTimestamp]) => prop(xs) })

    check(forAll { (xs: Vector[Option[Int]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[Long]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[Double]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[Float]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[Short]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[Byte]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[Boolean]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[String]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[SQLDate]]) => prop(xs) })
    check(forAll { (xs: Vector[Option[SQLTimestamp]]) => prop(xs) })
  }
}
