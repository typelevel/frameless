package frameless
package ops
package unoptimized

import org.scalacheck.Prop
import org.scalacheck.Prop._

class MapPartitionsTests extends TypedDatasetSuite {
  test("mapPartitions") {
    def prop[A: TypedEncoder, B: TypedEncoder](mapFunction: A => B, data: Vector[A]): Prop = {
      val lifted: Iterator[A] => Iterator[B] = _.map(mapFunction)
      TypedDataset.create(data).mapPartitions(lifted).collect().run().toVector =? data.map(mapFunction)
    }

    check(forAll(prop[Int, Int] _))
    check(forAll(prop[Int, String] _))
    check(forAll(prop[String, Int] _))
  }
}
