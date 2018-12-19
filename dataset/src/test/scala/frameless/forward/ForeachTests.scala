package frameless
package forward

import org.apache.spark.util.CollectionAccumulator

import org.scalacheck.Prop
import org.scalacheck.Prop._

import scala.collection.JavaConverters._

class ForeachTests extends TypedDatasetSuite {
  test("foreach") {
    def prop[A: Ordering: TypedEncoder](data: Vector[A]): Prop = {
      val accu = new CollectionAccumulator[A]()
      sc.register(accu)

      TypedDataset.create(data).foreach(accu.add).run()

      accu.value.asScala.toVector.sorted ?= data.sorted
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }

  test("foreachPartition") {
    def prop[A: Ordering: TypedEncoder](data: Vector[A]): Prop = {
      val accu = new CollectionAccumulator[A]()
      sc.register(accu)

      TypedDataset.create(data).foreachPartition(_.foreach(accu.add)).run()

      accu.value.asScala.toVector.sorted ?= data.sorted
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
