package frameless

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop._

class StorageLevelTests extends TypedDatasetSuite {

  val storageLevelGen: Gen[StorageLevel] = Gen.oneOf(
    Seq(
      NONE,
      DISK_ONLY,
      DISK_ONLY_2,
      MEMORY_ONLY,
      MEMORY_ONLY_2,
      MEMORY_ONLY_SER,
      MEMORY_ONLY_SER_2,
      MEMORY_AND_DISK,
      MEMORY_AND_DISK_2,
      MEMORY_AND_DISK_SER,
      MEMORY_AND_DISK_SER_2,
      OFF_HEAP
    ))

  test("storageLevel") {
    def prop[A: TypedEncoder: Arbitrary] = forAll(vectorGen[A], storageLevelGen) {
      (data: Vector[A], storageLevel: StorageLevel) =>
        val dataset = TypedDataset.create(data)
        if (storageLevel != StorageLevel.NONE)
          dataset.persist(storageLevel)

        dataset.count().run()

        dataset.storageLevel() ?= dataset.dataset.storageLevel
    }

    check(prop[Int])
    check(prop[String])
  }
}
