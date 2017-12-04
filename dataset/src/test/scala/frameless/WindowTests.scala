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
    val inputSeq = Seq(
      Foo(1, "hello"),
      Foo(2, "hello"),
      Foo(3, "hello"),
      Foo(1, "bye"),
      Foo(2, "bye")
    )

    val ds = TypedDataset.create(inputSeq)

    ds.show().run()

    val untypedWindow = Window.partitionBy("b").orderBy("a")

    /*
    root
     |-- a: integer (nullable = false)
     |-- b: string (nullable = true)
     |-- rank: integer (nullable = true)

    +---+-----+----+
    |  a|    b|rank|
    +---+-----+----+
    |  1|hello|   1|
    |  2|hello|   2|
    |  3|hello|   3|
    |  1|  bye|   1|
    |  2|  bye|   2|
    +---+-----+----+
     */
    ds.toDF()
      .withColumn("rank", sfuncs.dense_rank().over(untypedWindow))
      .show()

    val denseRankWindowed = dense_rank().over(
      TypedWindow
        .orderBy(ds[Int]('a))//.asc)
        .partitionBy(ds('b))
//        .partitionBy(functions.aggregate.sum(ds('a)))
//        .orderByMany(ds('a).desc, ds('b))
//        .orderBy(TypedSortedColumn.defaultAscending(ds('a)))//.asc)
        .orderBy(ds[Int]('a))//.asc)
    )


    ds.withColumnTupled(
      denseRankWindowed
    ).show().run()

    ds.withColumn[FooRank](
      denseRankWindowed
    ).show().run()




  }

}
