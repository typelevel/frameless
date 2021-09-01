package frameless
package syntax

import org.scalacheck.Prop
import org.scalacheck.Prop._
import frameless.functions.aggregate._

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
    def prop[A, B](data: Vector[X2[A, B]])(
        implicit ev: TypedEncoder[X2[A, B]]
    ): Prop = {
      val dataset = session.createDataset(data)(TypedExpressionEncoder(ev)).typed
      val dataframe = dataset.toDF()

      dataset
        .collect()
        .run()
        .toVector ?= dataframe.unsafeTyped[X2[A, B]].collect().run().toVector
    }

    check(forAll(prop[Int, String] _))
    check(forAll(prop[X1[Long], String] _))
  }

  test("frameless typed column and aggregate") {
    def prop[A: TypedEncoder](a: A, b: A): Prop = {
      val d = TypedDataset.create((a, b) :: Nil)
      (d.select(d('_1).untyped.typedColumn).collect().run ?= d.select(d('_1)).collect().run).&&(
        d.agg(first(d('_1))).collect().run() ?= d
          .agg(first(d('_1)).untyped.typedAggregate)
          .collect()
          .run()
      )
    }

    check(forAll(prop[Int] _))
    check(forAll(prop[X1[Long]] _))
  }
}
