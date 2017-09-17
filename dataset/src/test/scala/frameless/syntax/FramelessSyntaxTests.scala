package frameless.syntax

import frameless.{TypedDataset, TypedDatasetSuite, TypedEncoder, X2}
import org.scalacheck.Prop
import org.scalacheck.Prop._

class FramelessSyntaxTests extends TypedDatasetSuite {
  // Hide the implicit SparkDelay[Job] on TypedDatasetSuite to avoid ambiguous implicits
  override val sparkDelay = null

  def prop[A, B](data: Vector[X2[A, B]])(
    implicit ev: TypedEncoder[X2[A, B]]
  ): Prop = {
    val dataset = TypedDataset.create(data).dataset
    val dataframe = dataset.toDF()

    val typedDataset = dataset.typed
    val typedDatasetFromDataFrame = dataframe.unsafeTyped[X2[A, B]]

    typedDataset.collect().run().toVector ?= typedDatasetFromDataFrame.collect().run().toVector
  }

  test("dataset typed - toTyped") {
    check(forAll(prop[Int, String] _))
  }

}
