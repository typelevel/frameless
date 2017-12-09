package frameless

import org.apache.spark.sql.{ functions => sfunc }
import shapeless.test.illTyped

object SortTests {
  case class Wack(w: Int)
  case class Foo(a: String, b: Option[Int], c: Array[String], d: Map[String, Int], wack: Wack)
}

//TODO:
class SortTests extends TypedDatasetSuite {
  import SortTests._

  test("sorting") {
    val seq = Seq(
      Foo("a", Some(2), Array("a", "b"), Map("world" -> 2), Wack(1)),
      Foo("b", Some(1), Array("b", "a"), Map("world" -> 2), Wack(2))
    )

    val ds = TypedDataset.create(seq)

    assert(ds.sort(ds('a).asc).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("a").asc).collect().map(_.a))
    assert(ds.sort(ds('a).desc).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("a").desc).collect().map(_.a))

    assert(ds.sort(ds('b).asc).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("b").asc).collect().map(_.a))
    assert(ds.sort(ds('b).desc).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("b").desc).collect().map(_.a))

    assert(ds.sort(ds('b).ascNullsFirst).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("b").asc_nulls_first).collect().map(_.a))
    assert(ds.sort(ds('a), ds('b).desc).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("a"), sfunc.col("b").desc).collect().map(_.a))

    illTyped {
      //Maps aren't allow
      """ds.sort(ds('d).desc)"""
    }

    assert(ds.sort(ds('wack).desc).collect().run().map(_.a) === ds.dataset.sort(sfunc.col("wack").desc).collect().map(_.a))
  }

}
