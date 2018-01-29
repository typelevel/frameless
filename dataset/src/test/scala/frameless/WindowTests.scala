package frameless

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => sfuncs}
import frameless.functions.WindowFunctions._

object WindowTests {
  case class Foo(a: Int, b: String)
  case class FooRank(a: Int, b: String, rank: Int)
}
class WindowTests extends TypedDatasetSuite {
  import WindowTests._

  test("basic") {
    val spark = session
    import spark.implicits._

    val inputSeq = Seq(
      Foo(1, "a"),
      Foo(1, "b"),
      Foo(1, "c"),
      Foo(1, "d"),
      Foo(2, "a"),
      Foo(2, "b"),
      Foo(2, "c"),
      Foo(3, "c")
    )

    val ds = TypedDataset.create(inputSeq)

    val untypedWindow = Window.partitionBy("a").orderBy("b")

    val untyped = ds.toDF()
      .withColumn("rank", sfuncs.dense_rank().over(untypedWindow))
      .as[FooRank]
      .collect()
      .toList

    val denseRankWindowed = denseRank(
      TypedWindow
        //TODO: default won't work unless `ds.apply` is typed. Or could just call `.asc`
        .orderBy(ds[String]('b))
        .partitionBy(ds('a))
    )

    val typed = ds.withColumn[FooRank](
      denseRankWindowed
    ).collect()
      .run()
      .toList

    assert(untyped === typed)

  }

}
