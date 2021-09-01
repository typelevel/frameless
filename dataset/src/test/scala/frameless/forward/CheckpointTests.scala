package frameless

import org.scalacheck.Prop
import org.scalacheck.Prop.{forAll, _}

class CheckpointTests extends TypedDatasetSuite {
  test("checkpoint") {
    def prop[A: TypedEncoder](data: Vector[A], isEager: Boolean): Prop = {
      val dataset = TypedDataset.create(data)

      dataset.sparkSession.sparkContext.setCheckpointDir(TEST_OUTPUT_DIR)

      dataset.checkpoint(isEager).run().queryExecution.toString() =?
        dataset.dataset.checkpoint(isEager).queryExecution.toString()
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[String] _))
  }
}
