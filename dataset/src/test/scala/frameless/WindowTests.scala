package frameless

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ functions => sfuncs }
import frameless.functions.WindowFunctions._
import org.scalacheck.Prop
import org.scalacheck.Prop._

object WindowTests {
  case class Foo(a: Int, b: String)
  case class FooRank(a: Int, b: String, rank: Int)
}

class WindowTests extends TypedDatasetSuite {
  test("dense rank") {
    def prop[
      A : TypedEncoder,
      B : TypedEncoder : CatalystOrdered
    ](data: Vector[X2[A, B]]): Prop = {
      val ds = TypedDataset.create(data)

      val untypedWindow = Window.partitionBy("a").orderBy("b")

      val untyped = TypedDataset.createUnsafe[X3[A, B, Int]](ds.toDF()
        .withColumn("c", sfuncs.dense_rank().over(untypedWindow))
      ).collect().run().toVector

      val denseRankWindow = denseRank(TypedWindow.orderBy(ds[B]('b))
          .partitionBy(ds('a)))

      val typed = ds.withColumn[X3[A, B, Int]](denseRankWindow)
        .collect().run().toVector

      typed ?= untyped
    }

    check(forAll(prop[Int, String] _))
    check(forAll(prop[SQLDate, SQLDate] _))
    check(forAll(prop[String, Boolean] _))
  }
}
